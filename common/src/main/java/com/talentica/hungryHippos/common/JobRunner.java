package com.talentica.hungryHippos.common;

import java.io.Serializable;
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
public class JobRunner implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4793614653018059851L;
	private Map<Integer, TaskEntity> taskIdToTaskEntitiesMap = new HashMap<Integer, TaskEntity>();
	private DataStore dataStore;
	private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class.getName());
	private DynamicMarshal dynamicMarshal = null;
	private DataRowProcessor rowProcessor = null;

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
		this.dataStore = dataStore;
		dynamicMarshal = new DynamicMarshal(dataDescription);
		rowProcessor = new DataRowProcessor(dynamicMarshal);
	}

	private Map<Integer, List<TaskEntity>> primaryDimTasksMap = new HashMap<>();

	public void addTask(TaskEntity taskEntity) {
		taskIdToTaskEntitiesMap.put(taskEntity.getTaskId(), taskEntity);
		Integer primDim = taskEntity.getJobEntity().getJob().getPrimaryDimension();
		List<TaskEntity> primDimList = primaryDimTasksMap.get(primDim);
		if (primDimList == null) {
			primDimList = new LinkedList<>();
			primaryDimTasksMap.put(primDim, primDimList);
		}
		primDimList.add(taskEntity);
	}

	public void run() {
		RowProcessor rowProcessor = null;
		StoreAccess storeAccess = null;
		for (Integer primDim : primaryDimTasksMap.keySet()) {
			storeAccess = dataStore.getStoreAccess(primDim);
			for (TaskEntity task : primaryDimTasksMap.get(primDim)) {
				if (rowProcessor == null) {
					rowProcessor = new DataRowProcessor(dynamicMarshal, task.getJobEntity().getJob().getDimensions());
					storeAccess.addRowProcessor(rowProcessor);
				}
				DataRowProcessor dataRowProcessor = (DataRowProcessor) rowProcessor;
				dataRowProcessor.putReducer(task.getValueSet(), task.getWork());
			}
			storeAccess.processRows();
		}
		if (rowProcessor != null)
			rowProcessor.finishUp();
	}

	/**
	 * Counts the number of rows need to process for each jobs.
	 * 
	 * @return Map<Integer, JobEntity>
	 */
	public void doRowCount(JobEntity jobToExecute) {
		long startTimeRowCount = System.currentTimeMillis();
		LOGGER.info("Row count for all jobs started");
		int primaryDimension = jobToExecute.getJob().getPrimaryDimension();
		StoreAccess storeAccess = dataStore.getStoreAccess(primaryDimension);
		rowProcessor.setJob(jobToExecute);
		storeAccess.addRowProcessor(rowProcessor);
		storeAccess.processRowCount();
		for (TaskEntity taskEntity : rowProcessor.getWorkerValueSet().values()) {
			taskIdToTaskEntitiesMap.put(taskEntity.getTaskId(), taskEntity);
			LOGGER.debug("JOB ID {} AND TASK ID {} AND ValueSet {} AND ROW COUNT {}  AND MEMORY FOOTPRINT {}",
					"JOB ID {} AND ValueSet {} Index {} AND ROW COUNT {}  AND MEMORY FOOTPRINT {}",
					taskEntity.getJobEntity().getJobId(), taskEntity.getValueSet(),
					taskEntity.getJobEntity().getJob().getIndex(), taskEntity.getRowCount(),
					taskEntity.getJobEntity().getJob().getMemoryFootprint(taskEntity.getRowCount()));
		}
		long endTime = System.currentTimeMillis();
		LOGGER.info("Row count for job: {} finished in {} ms",
				new Object[] { jobToExecute, (endTime - startTimeRowCount) });
	}

	public Map<Integer, TaskEntity> getTaskEntities() {
		return taskIdToTaskEntitiesMap;
	}

	public void clear() {
		primaryDimTasksMap.clear();
		taskIdToTaskEntitiesMap.clear();
	}

}
