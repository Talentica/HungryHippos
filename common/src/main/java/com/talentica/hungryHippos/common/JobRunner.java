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
	
	private static Map<Integer,Map<IntArrayKeyHashMap,List<JobEntity>>> primDimenAsKeyDimensEntityMap = new HashMap<>();

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
		this.dataStore = dataStore;
		dynamicMarshal = new DynamicMarshal(dataDescription);
	}

	public void addJob(JobEntity jobEntity){
		Map<IntArrayKeyHashMap,List<JobEntity>> dimensJobEntityList = primDimenAsKeyDimensEntityMap.get(jobEntity.getJob().getPrimaryDimension());
		if(dimensJobEntityList == null){
			dimensJobEntityList = new HashMap<>();
			primDimenAsKeyDimensEntityMap.put(jobEntity.getJob().getPrimaryDimension(),dimensJobEntityList);
		}
		IntArrayKeyHashMap dimensAsKey = new IntArrayKeyHashMap(jobEntity.getJob().getDimensions());
		List<JobEntity> primDimenJobEntityList = dimensJobEntityList.get(dimensAsKey);
		if(primDimenJobEntityList == null){
			primDimenJobEntityList = new ArrayList<>();
			dimensJobEntityList.put(dimensAsKey,primDimenJobEntityList);
		}
		primDimenJobEntityList.add(jobEntity);
	}
	
	public void run() {
		checkIfMemoryAvailableToRunJob();
		StoreAccess storeAccess = null;
		for (int primDimenAsKey : primDimenAsKeyDimensEntityMap.keySet()) {
			storeAccess = dataStore.getStoreAccess(primDimenAsKey);
			Map<IntArrayKeyHashMap, List<JobEntity>> dimensWiseJobEntityMap = primDimenAsKeyDimensEntityMap
					.get(primDimenAsKey);
			for (IntArrayKeyHashMap dimensAsKey : dimensWiseJobEntityMap
					.keySet()) {
				DataRowProcessor rowProcessor = new DataRowProcessor(
						dynamicMarshal,
						dimensWiseJobEntityMap.get(dimensAsKey), dimensAsKey);
				storeAccess.addRowProcessor(rowProcessor);
				do {
					storeAccess.processRows();
					rowProcessor.finishUp();
				} while (rowProcessor
						.isAdditionalValueSetsPresentForProcessing());
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
