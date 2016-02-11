package com.talentica.hungryHippos.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class JobRunner implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -4793614653018059851L;
	List<Job> jobs= new LinkedList<>();
	List<TaskEntity> taskEntities= new ArrayList<TaskEntity>();
    private DataDescription dataDescription;
    private DataStore dataStore;
    private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class.getName());

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
        this.dataDescription = dataDescription;
        this.dataStore = dataStore;
    }

    private Map<Integer,List<Job>> primaryDimJobsMap = new HashMap<>();
    private Map<Integer,List<TaskEntity>> primaryDimTasksMap = new HashMap<>();
    private Map<int[],RowProcessor> dimensionsRowProcessorMap = new HashMap<int[],RowProcessor>();
    private HashMap<ValueSet, Work> valueSetWorkMap = new HashMap<>();

    public void addJob(Job job){
        jobs.add(job);
        Integer primDim = job.getPrimaryDimension();
        List<Job> primDimList = primaryDimJobsMap.get(primDim);
        if(primDimList == null){
            primDimList = new LinkedList<>();
            primaryDimJobsMap.put(primDim,primDimList);
        }
        primDimList.add(job);
    }  
    
    public void addJobs(List<Job> jobs){
    	for(Job job : jobs){
    		addJob(job);
    	}
    }
    
    public void addTask(TaskEntity taskEntity){
    	taskEntities.add(taskEntity);
    	Integer primDim = taskEntity.getWork().getPrimaryDimension();
        List<TaskEntity> primDimList = primaryDimTasksMap.get(primDim);
        if(primDimList == null){
            primDimList = new LinkedList<>();
            primaryDimTasksMap.put(primDim,primDimList);
        }
        primDimList.add(taskEntity);
    }
    
    
    public void run(){
    	LOGGER.info("EXECUTION STARTED FOR ABOVE TASKS");
    	Stopwatch timer = new Stopwatch().start();
        DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
        List<RowProcessor> rowProcessors = new LinkedList<>();
        StoreAccess storeAccess = null;
	        	for(Integer primDim: primaryDimTasksMap.keySet()){
	        		storeAccess = dataStore.getStoreAccess(primDim);
		        	for(TaskEntity task : primaryDimTasksMap.get(primDim)){
		        		RowProcessor rowProcessor = dimensionsRowProcessorMap.get(task.getWork().getDimensions());
		        		if(rowProcessor == null){
		        			valueSetWorkMap = new HashMap<>();
		        			rowProcessor = new DataRowProcessor(dynamicMarshal,valueSetWorkMap,task.getWork().getDimensions());	
		        			dimensionsRowProcessorMap.put(task.getWork().getDimensions(), rowProcessor);
		        			storeAccess.addRowProcessor(rowProcessor);
			                rowProcessors.add(rowProcessor);
		        		}
		        		DataRowProcessor dataRowProcessor = (DataRowProcessor)rowProcessor;
		        		dataRowProcessor.getValueSetWorkMap().put(task.getValueSet(), task.getWork());
		        	}
	        		 storeAccess.processRows();
	        	}
        rowProcessors.forEach(RowProcessor::finishUp);
        timer.stop();
        LOGGER.info("TIME TAKEN TO EXECUTE ABOVE TASKS {} ms",(timer.elapsedMillis()));
}
    
    /**
     * Counts the number of rows need to process for each jobs.
     * 
     * @return Map<Integer, JobEntity>
     */
    public void doRowCount(){
    	LOGGER.info("ROW COUNTS FOR ALL JOBS STARTED");
    	Stopwatch timer = new Stopwatch().start();
    	 DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    	 List<RowProcessor> rowProcessors = new LinkedList<>();
        for(Integer primDim: primaryDimJobsMap.keySet()){
			StoreAccess storeAccess = dataStore.getStoreAccess(primDim);
            for(Job job:primaryDimJobsMap.get(primDim)) {
                RowProcessor rowProcessor = new DataRowProcessor(dynamicMarshal,job);
                storeAccess.addRowProcessor(rowProcessor);
                rowProcessors.add(rowProcessor);
               }
            storeAccess.processRowCount();
            for(RowProcessor processor : rowProcessors){
            	DataRowProcessor dataRowProcessor = (DataRowProcessor)processor;
            	for(TaskEntity taskEntity : dataRowProcessor.getWorkerValueSet().values()){
            		taskEntities.add(taskEntity);
					LOGGER.info("JOB ID {} AND TASK ID {} AND ValueSet {} AND ROW COUNT {}  AND MEMORY FOOTPRINT {}",
							taskEntity.getJob().getJobId(), taskEntity.getTaskId(), taskEntity.getValueSet(),
							taskEntity.getRowCount(), taskEntity.getJob().getMemoryFootprint(taskEntity.getRowCount()));
            	}
            }
            rowProcessors.clear();
        }
        timer.stop();
        LOGGER.info("TOTAL TIME TAKEN FOR ROW COUNTS OF ALL JOBS {} ms",(timer.elapsedMillis()));
    }
    
    
    
    public List<TaskEntity> getWorkEntities() {
		return taskEntities;
	}

	public void clear(){
    	jobs.clear();
		primaryDimJobsMap.clear();
		primaryDimTasksMap.clear();
		taskEntities.clear();
		dimensionsRowProcessorMap.clear();
		valueSetWorkMap.clear();
    }
    

}
