package com.talentica.hungryHippos.accumulator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.Node;
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
    private Map<String,Map<Object, Node>> keyValueNodeNumberMap ;
    private DataDescription dataDescription;
    private DataStore dataStore;
    private Map<Integer,JobEntity> jobIdJobEntityCount = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class.getName());

    public JobRunner(DataDescription dataDescription, DataStore dataStore, Map<String,Map<Object, Node>> keyValueNodeNumberMap) {
        this.dataDescription = dataDescription;
        this.dataStore = dataStore;
        this.keyValueNodeNumberMap = keyValueNodeNumberMap;
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
            System.out.println(keyValueNodeNumberMap);
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
    
    public Map<Integer, JobEntity> getJobIdJobEntityMap(){
        List<RowProcessor> rowProcessors = new LinkedList<>();
        for(Integer primDim: primaryDimJobsMap.keySet()){
            StoreAccess storeAccess = dataStore.getStoreAccess(primDim);
            for(Job job:primaryDimJobsMap.get(primDim)) {
                RowProcessor rowProcessor = new DataRowProcessor(job);
                storeAccess.addRowProcessor(rowProcessor);
                rowProcessors.add(rowProcessor);
            }
            storeAccess.processRowCount();
            for(RowProcessor processor : rowProcessors){
            	Job job = ((Job)processor.getJob());
            	JobEntity jobEntity = (JobEntity) processor.getJobEntity();
            	if(jobEntity.getRowCount() == 0l) continue;
            	jobIdJobEntityCount.put(job.getJobId(),jobEntity);
            	LOGGER.info("Job Id {} and Row count {}",job.getJobId(),jobEntity.getRowCount());
            }
            rowProcessors.clear();
        }
        return jobIdJobEntityCount;
    }
    
    public List<Job> getJobs(){
    	return jobs;
    }
    
    public void clearJobList(){
    	jobs.clear();
    }
    

}
