package com.talentica.hungryHippos.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
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
	private List<JobEntity> jobEntities = new LinkedList<>();
	private List<TaskEntity> taskEntities = new ArrayList<TaskEntity>();
	private DataDescription dataDescription;
	private DataStore dataStore;
	private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class.getName());

	public JobRunner(DataDescription dataDescription, DataStore dataStore) {
		this.dataDescription = dataDescription;
		this.dataStore = dataStore;
	}

	private Map<Integer, List<JobEntity>> primaryDimJobsMap = new HashMap<>();
	private Map<Integer, List<TaskEntity>> primaryDimTasksMap = new HashMap<>();
	private Map<int[], RowProcessor> dimensionsRowProcessorMap = new HashMap<int[], RowProcessor>();
	private HashMap<ValueSet, Work> valueSetWorkMap = new HashMap<>();

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
		/*
		 * for (JobEntity jobEntity : jobEntities) { addJob(jobEntity); }
		 */
		addJob(jobEntity);
	}

	public void addTask(TaskEntity taskEntity) {
		taskEntities.add(taskEntity);
		Integer primDim = taskEntity.getWork().getPrimaryDimension();
		List<TaskEntity> primDimList = primaryDimTasksMap.get(primDim);
		if (primDimList == null) {
			primDimList = new LinkedList<>();
			primaryDimTasksMap.put(primDim, primDimList);
		}
		primDimList.add(taskEntity);
	}

	public void run() {
		LOGGER.info("EXECUTION STARTED FOR ABOVE TASKS");
		Stopwatch timer = new Stopwatch().start();
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		List<RowProcessor> rowProcessors = new LinkedList<>();
		StoreAccess storeAccess = null;
		for (Integer primDim : primaryDimTasksMap.keySet()) {
			storeAccess = dataStore.getStoreAccess(primDim);
			for (TaskEntity task : primaryDimTasksMap.get(primDim)) {
				RowProcessor rowProcessor = dimensionsRowProcessorMap.get(task.getWork().getDimensions());
				if (rowProcessor == null) {
					valueSetWorkMap = new HashMap<>();
					rowProcessor = new DataRowProcessor(dynamicMarshal, valueSetWorkMap,
							task.getWork().getDimensions());
					dimensionsRowProcessorMap.put(task.getWork().getDimensions(), rowProcessor);
					storeAccess.addRowProcessor(rowProcessor);
					rowProcessors.add(rowProcessor);
				}

				valueSetWorkMap.put(task.getValueSet(), task.getWork());
			}
			storeAccess.processRows();
		}
		rowProcessors.forEach(RowProcessor::finishUp);
		timer.stop();
		LOGGER.info("TIME TAKEN TO EXECUTE ABOVE TASKS {} ms", (timer.elapsedMillis()));
	}

	public void doRowCount() {
		LOGGER.info("ROW COUNTS FOR ALL JOBS STARTED");
		long startTime = System.currentTimeMillis();
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		for (Integer primDim : primaryDimJobsMap.keySet()) {
			StoreAccess storeAccess = dataStore.getStoreAccess(primDim);
			DataRowProcessor rowProcessor = new DataRowProcessor(dynamicMarshal);
			for (JobEntity jobEntity : primaryDimJobsMap.get(primDim)) {
				rowProcessor.setJob(jobEntity);
				storeAccess.addRowProcessor(rowProcessor);
				storeAccess.processRowCount();
				LOGGER.info("WorkValueSet map size for primary dimension {} is {}",
						new Object[] { primDim, rowProcessor.getWorkerValueSet().size() });
				for (TaskEntity taskEntity : rowProcessor.getWorkerValueSet().values()) {
					taskEntities.add(taskEntity);
					LOGGER.info("JOB ID {} AND TASK ID {} AND ValueSet {} AND ROW COUNT {}  AND MEMORY FOOTPRINT {}",
							taskEntity.getJobEntity().getJobId(), taskEntity.getTaskId(), taskEntity.getValueSet(),
							taskEntity.getRowCount(),
							taskEntity.getJobEntity().getJob().getMemoryFootprint(taskEntity.getRowCount()));
				}
			}
		}
		long endTime = System.currentTimeMillis();
		LOGGER.info("TOTAL TIME TAKEN FOR ROW COUNTS OF ALL JOBS {} ms", (endTime - startTime));
	}

	public List<TaskEntity> getWorkEntities() {
		return taskEntities;
	}

	public void clear() {
		jobEntities.clear();
		primaryDimJobsMap.clear();
		primaryDimTasksMap.clear();
		taskEntities.clear();
		dimensionsRowProcessorMap.clear();
		valueSetWorkMap.clear();
	}

}
