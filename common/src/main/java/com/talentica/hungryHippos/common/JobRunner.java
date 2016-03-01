package com.talentica.hungryHippos.common;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.JobEntity;
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

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
		this.dataStore = dataStore;
		dynamicMarshal = new DynamicMarshal(dataDescription);
	}

	public void run(JobEntity jobEntity) {
		RowProcessor rowProcessor = null;
		StoreAccess storeAccess = null;
		storeAccess = dataStore.getStoreAccess(jobEntity.getJob().getPrimaryDimension());
		rowProcessor = new DataRowProcessor(dynamicMarshal, jobEntity);
		storeAccess.addRowProcessor(rowProcessor);
		storeAccess.processRows();
		if (rowProcessor != null)
			rowProcessor.finishUp();
	}

}
