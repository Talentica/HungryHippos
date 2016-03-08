package com.talentica.hungryHippos.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class JobRunner implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4793614653018059851L;
	private DataStore dataStore;
	private DynamicMarshal dynamicMarshal = null;
	private Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);
	private Map<Integer,Map<IntArrayKeyHashMap,List<JobEntity>>> primDimenAsKeyAndDimensJobEntityMap = new HashMap<Integer,Map<IntArrayKeyHashMap,List<JobEntity>>>();
	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
		this.dataStore = dataStore;
		dynamicMarshal = new DynamicMarshal(dataDescription);
	}

	public void addJob(JobEntity jobEntity){
		Map<IntArrayKeyHashMap,List<JobEntity>> dimensJobListMap = primDimenAsKeyAndDimensJobEntityMap.get(jobEntity.getJob().getPrimaryDimension());
		IntArrayKeyHashMap dimensAsKey = new IntArrayKeyHashMap(jobEntity.getJob().getDimensions());
		if(dimensJobListMap == null){
			dimensJobListMap = new HashMap<>();
			primDimenAsKeyAndDimensJobEntityMap.put(jobEntity.getJob().getPrimaryDimension(), dimensJobListMap);
		}
		List<JobEntity> jobEntitiesOfLikeDimens = dimensJobListMap.get(dimensAsKey);
		if(jobEntitiesOfLikeDimens == null){
			jobEntitiesOfLikeDimens = new ArrayList<>();
			dimensJobListMap.put(dimensAsKey,jobEntitiesOfLikeDimens);
		}
		jobEntitiesOfLikeDimens.add(jobEntity);
		
	}
	
	public void run() {
		checkIfMemoryAvailableToRunJob();
		StoreAccess storeAccess = null;
		for (Integer primDimAsKey : primDimenAsKeyAndDimensJobEntityMap.keySet()) {
			storeAccess = dataStore.getStoreAccess(primDimAsKey);
			for (IntArrayKeyHashMap dimesAsKey : primDimenAsKeyAndDimensJobEntityMap.get(primDimAsKey).keySet()) {
				List<JobEntity> jobEntitiesOfLikeDimens = primDimenAsKeyAndDimensJobEntityMap.get(primDimAsKey).get(dimesAsKey);
				DataRowProcessor dataRowProcessor = new DataRowProcessor(dynamicMarshal, jobEntitiesOfLikeDimens,dimesAsKey.getValues());
					LOGGER.info("Starting execution of no. of jobs {}" , jobEntitiesOfLikeDimens.size());
					storeAccess.addRowProcessor(dataRowProcessor);
					do {
						storeAccess.processRows();
						dataRowProcessor.finishUp();
					} while (dataRowProcessor.isAdditionalValueSetsPresentForProcessing());
			}
		}
	}

	private void checkIfMemoryAvailableToRunJob() {
		long freeMemory = MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated();
		if (freeMemory <= DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS) {
			LOGGER.error(
					"Either very less memory:{} MBs is available to run jobs or the amount of threshold memory:{} MBs configured is too high.",
					new Object[] { freeMemory, DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS });
			throw new RuntimeException(
					"Either very less memory is available to run jobs or the amount of threshold memory configured is too high.");
		}
	}

}
