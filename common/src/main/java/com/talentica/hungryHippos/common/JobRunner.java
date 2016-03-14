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
	private Map<Integer,List<JobEntity>> primDimesJobEntityListMap =  new HashMap<Integer,List<JobEntity>>();
	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
		this.dataStore = dataStore;
		dynamicMarshal = new DynamicMarshal(dataDescription);
	}

	public void addJob(JobEntity jobEntity){
		List<JobEntity> primDimenJobEntityList = primDimesJobEntityListMap.get(jobEntity.getJob().getPrimaryDimension());
		if(primDimenJobEntityList == null){
			primDimenJobEntityList = new ArrayList<>();
			primDimesJobEntityListMap.put(jobEntity.getJob().getPrimaryDimension(),primDimenJobEntityList);
		}
		primDimenJobEntityList.add(jobEntity);
	}
	
	public void run() {
		checkIfMemoryAvailableToRunJob();
		StoreAccess storeAccess = null;
		for (Integer primDimAsKey : primDimesJobEntityListMap.keySet()) {
			storeAccess = dataStore.getStoreAccess(primDimAsKey);
			DataRowProcessor dataRowProcessor = new DataRowProcessor(dynamicMarshal, primDimesJobEntityListMap.get(primDimAsKey));
			storeAccess.addRowProcessor(dataRowProcessor);
			do {
				storeAccess.processRows();
				dataRowProcessor.finishUp();
			} while (dataRowProcessor.isAdditionalValueSetsPresentForProcessing());
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
