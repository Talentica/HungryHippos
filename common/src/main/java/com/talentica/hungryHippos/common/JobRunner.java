package com.talentica.hungryHippos.common;

import java.io.Serializable;

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

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
		this.dataStore = dataStore;
		dynamicMarshal = new DynamicMarshal(dataDescription);
	}

	public void run(JobEntity jobEntity) {
		checkIfMemoryAvailableToRunJob();
		StoreAccess storeAccess = null;
		storeAccess = dataStore.getStoreAccess(jobEntity.getJob().getPrimaryDimension());
		DataRowProcessor rowProcessor = new DataRowProcessor(dynamicMarshal, jobEntity);
		storeAccess.addRowProcessor(rowProcessor);
		// do {
			storeAccess.processRows();
		rowProcessor.finishUp();
		// } while (rowProcessor.isAdditionalValueSetsPresentForProcessing());
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
