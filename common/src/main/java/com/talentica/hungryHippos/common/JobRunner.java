package com.talentica.hungryHippos.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class JobRunner implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -4793614653018059851L;
	List<JobEntity> jobEntities= new LinkedList<>();
	List<TaskEntity> taskEntities= new ArrayList<TaskEntity>();
    private DataDescription dataDescription;
    private DataStore dataStore;
    private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class.getName());

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
        this.dataDescription = dataDescription;
        this.dataStore = dataStore;
    }

    private Map<Integer,List<JobEntity>> primaryDimJobsMap = new HashMap<>();
    private Map<Integer,List<TaskEntity>> primaryDimTasksMap = new HashMap<>();
    private Map<IntArrayKeyHashMap,RowProcessor> dimensionsRowProcessorMap = new HashMap<IntArrayKeyHashMap,RowProcessor>();

    public void addJob(JobEntity jobEntity){
        jobEntities.add(jobEntity);
        Integer primDim = jobEntity.getJob().getPrimaryDimension();
        List<JobEntity> primDimList = primaryDimJobsMap.get(primDim);
        if(primDimList == null){
            primDimList = new LinkedList<>();
            primaryDimJobsMap.put(primDim,primDimList);
        }
        primDimList.add(jobEntity);
    }
    
    public void addJobs(JobEntity jobEntity){
    	addJob(jobEntity);
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
    	LOGGER.info("BATCH EXECUTION STARTED FOR BATCH SIZE {}",taskEntities.size());
    	long startTime = System.currentTimeMillis();
        DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
        RowProcessor rowProcessor = null;
        StoreAccess storeAccess = null;
	        	for(Integer primDim: primaryDimTasksMap.keySet()){
	        		storeAccess = dataStore.getStoreAccess(primDim);
		        	for(TaskEntity task : primaryDimTasksMap.get(primDim)){
		        		if(rowProcessor == null ){
		        			rowProcessor = new DataRowProcessor(dynamicMarshal,task.getWork().getDimensions());
		        			storeAccess.addRowProcessor(rowProcessor);
		        		}
		        		DataRowProcessor dataRowProcessor = (DataRowProcessor)rowProcessor;
		        		dataRowProcessor.putReducer(task.getValueSet(), task.getWork());
		        	}
	        		 storeAccess.processRows();
	        	}
	    rowProcessor.finishUp();
        LOGGER.info("BATCH COMPLETION TIME {} ms",(System.currentTimeMillis()-startTime));
}
    
    /**
     * Counts the number of rows need to process for each jobs.
     * 
     * @return Map<Integer, JobEntity>
     */
    public void doRowCount() {
		long startTime = System.currentTimeMillis();
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		for (Integer primDim : primaryDimJobsMap.keySet()) {
			StoreAccess storeAccess = dataStore.getStoreAccess(primDim);
			DataRowProcessor rowProcessor = new DataRowProcessor(dynamicMarshal);
			for (JobEntity jobEntity : primaryDimJobsMap.get(primDim)) {
				rowProcessor.setJob(jobEntity);
				storeAccess.addRowProcessor(rowProcessor);
				storeAccess.processRowCount();
				for (TaskEntity taskEntity : rowProcessor.getWorkerValueSet().values()) {
					taskEntities.add(taskEntity);
					/*LOGGER.info("JOB ID {} AND TASK ID {} AND ValueSet {} AND ROW COUNT {}  AND MEMORY FOOTPRINT {}",
							taskEntity.getJobEntity().getJobId(), taskEntity.getTaskId(), taskEntity.getValueSet(),
							taskEntity.getRowCount(),
							taskEntity.getJobEntity().getJob().getMemoryFootprint(taskEntity.getRowCount()));*/
				}
			}
		}
		long endTime = System.currentTimeMillis();
		LOGGER.info("TIME TAKEN FOR ABOVE JOB FOR ROW COUNT {} ms", (endTime - startTime));
	}

    
    
    
    public List<TaskEntity> getWorkEntities() {
		return taskEntities;
	}

	public void clear(){
    	jobEntities.clear();
		primaryDimJobsMap.clear();
		primaryDimTasksMap.clear();
		taskEntities.clear();
		dimensionsRowProcessorMap.clear();
    }
    

}
