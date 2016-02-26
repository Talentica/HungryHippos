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
	List<JobEntity> jobEntities = new LinkedList<>();
	private Map<Integer, TaskEntity> taskIdToTaskEntitiesMap = new HashMap<Integer, TaskEntity>();
	private DataDescription dataDescription;
	private DataStore dataStore;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(JobRunner.class.getName());

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
		this.dataDescription = dataDescription;
		this.dataStore = dataStore;
	}

	private Map<Integer, List<JobEntity>> primaryDimJobsMap = new HashMap<>();
	private Map<Integer, List<TaskEntity>> primaryDimTasksMap = new HashMap<>();

	public void addJob(JobEntity jobEntity) {
		jobEntities.add(jobEntity);
		Integer primDim = jobEntity.getJob().getPrimaryDimension();
		List<JobEntity> primDimList = primaryDimJobsMap.get(primDim);
		if (primDimList == null) {
			primDimList = new LinkedList<>();
			primaryDimJobsMap.put(primDim, primDimList);
		}
		primDimList.add(jobEntity);
	}

	public void addJobs(JobEntity jobEntity) {
		addJob(jobEntity);
	}

	public void addTask(TaskEntity taskEntity) {
		taskIdToTaskEntitiesMap.put(taskEntity.getTaskId(), taskEntity);
		Integer primDim = taskEntity.getJobEntity().getJob()
				.getPrimaryDimension();
		List<TaskEntity> primDimList = primaryDimTasksMap.get(primDim);
		if (primDimList == null) {
			primDimList = new LinkedList<>();
			primaryDimTasksMap.put(primDim, primDimList);
		}
		primDimList.add(taskEntity);
	}

	public void run() {
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		RowProcessor rowProcessor = null;
		StoreAccess storeAccess = null;
		for (Integer primDim : primaryDimTasksMap.keySet()) {
			storeAccess = dataStore.getStoreAccess(primDim);
			for (TaskEntity task : primaryDimTasksMap.get(primDim)) {
				if (rowProcessor == null) {
					rowProcessor = new DataRowProcessor(dynamicMarshal, task
							.getJobEntity().getJob().getDimensions());
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
	public void doRowCount() {
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		for (Integer primDim : primaryDimJobsMap.keySet()) {
			StoreAccess storeAccess = dataStore.getStoreAccess(primDim);
			DataRowProcessor rowProcessor = new DataRowProcessor(dynamicMarshal);
			for (JobEntity jobEntity : primaryDimJobsMap.get(primDim)) {
				rowProcessor.setJob(jobEntity);
				storeAccess.addRowProcessor(rowProcessor);
				storeAccess.processRowCount();
				for (TaskEntity taskEntity : rowProcessor.getWorkerValueSet()
						.values()) {
					taskIdToTaskEntitiesMap.put(taskEntity.getTaskId(), taskEntity);
					LOGGER.debug("JOB ID {} AND TASK ID {} AND ValueSet {} AND ROW COUNT {}  AND MEMORY FOOTPRINT {}",
							"JOB ID {} AND ValueSet {} Index {} AND ROW COUNT {}  AND MEMORY FOOTPRINT {}",
							taskEntity.getJobEntity().getJobId(),
							taskEntity.getValueSet(),
							taskEntity.getJobEntity().getJob().getIndex(),
							taskEntity.getRowCount(),
							taskEntity
									.getJobEntity()
									.getJob()
									.getMemoryFootprint(
											taskEntity.getRowCount()));
				}
			}
		}
	}

	public Map<Integer, TaskEntity> getWorkEntities() {
		return taskIdToTaskEntitiesMap;
	}

	public void clear() {
		jobEntities.clear();
		primaryDimJobsMap.clear();
		primaryDimTasksMap.clear();
		taskIdToTaskEntitiesMap.clear();
	}

}
