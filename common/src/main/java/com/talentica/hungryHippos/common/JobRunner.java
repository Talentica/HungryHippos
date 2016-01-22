package com.talentica.hungryHippos.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private DataDescription dataDescription;
    private DataStore dataStore;
    private Map<Integer,JobEntity> jobIdJobEntityCount = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class.getName());

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
        this.dataDescription = dataDescription;
        this.dataStore = dataStore;
    }

    private Map<Integer,List<Job>> primaryDimJobsMap = new HashMap<>();


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
    

    public void run(){
            DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
            List<RowProcessor> rowProcessors = new LinkedList<>();
            for(Integer primDim: primaryDimJobsMap.keySet()){
                StoreAccess storeAccess = dataStore.getStoreAccess(primDim);
                for(Job job:primaryDimJobsMap.get(primDim)) {
                    RowProcessor rowProcessor = new DataRowProcessor(dynamicMarshal,job);
                    storeAccess.addRowProcessor(rowProcessor);
                    rowProcessors.add(rowProcessor);
                }
                storeAccess.processRows();
            }
            rowProcessors.forEach(RowProcessor::finishUp);
    }
    
    /**
     * Counts the number of rows need to process for each jobs.
     * 
     * @return Map<Integer, JobEntity>
     */
    public List<JobEntity> getJobIdJobEntityMap(){
    	 DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    	 List<JobEntity> jobEntities = new ArrayList<JobEntity>();
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
            	Job job = ((Job)processor.getJob());
            	JobEntity jobEntity = (JobEntity) processor.getJobEntity();
            	if(jobEntity.getWorkerIdRowCountMap().size() == 0) continue;  // If no worker for job, just skip.
            	jobIdJobEntityCount.put(job.getJobId(),jobEntity);
            	LOGGER.info("JOB ID {} AND TOTAL WORKERS {}",job.getJobId(),jobEntity.getWorkerIdRowCountMap().size());
            	jobEntities.add(jobEntity);
            }
            rowProcessors.clear();
        }
        return jobEntities;
    }
    
    public List<Job> getJobs(){
    	return jobs;
    }
    
    public void clearJobList(){
    	jobs.clear();
    }
    

}
