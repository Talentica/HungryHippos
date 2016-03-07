package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataRowProcessor implements RowProcessor {

	private static final long MAXIMUM_NO_OF_ROWS_TO_PERFORM_GC_AFTER = Long
			.parseLong(Property.getPropertyValue("no.of.rows.to.run.gc.after").toString());

	private static final long MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER = Long
			.parseLong(Property.getPropertyValue("no.of.rows.to.log.progress.after").toString());

	private static final long NO_OF_ROWS_TO_CHECK_AVAILABLE_MEMORY_AFTER = Long
			.parseLong(Property.getPropertyValue("no.of.rows.to.check.available.memory.after").toString());

	private DynamicMarshal dynamicMarshal;

	private boolean garbageCollectionRan = false;

	private ExecutionContextImpl executionContext;

	private List<JobEntity> jobEntities;

	//private List<Class<? extends Work>> workClassTypeList = new ArrayList<>();

	private TreeMap<ValueSet, List<Work>> valuestWorkTreeMap = new TreeMap<>();

	private ValueSet maxValueSetOfCurrentBatch;

	private int[] keys;

	private boolean isCurrentBatchFull;

	private ValueSet processedTillValueSetInLastBatches = null;

	private boolean additionalValueSetsPresentForProcessing = false;

	private Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class);

	private int batchId = 0;

	private int countOfRows = 0;

	private int totalNoOfRowsProcessed = 0;

	public static final long MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS = Long
			.valueOf(Property.getPropertyValue("node.min.free.memory.in.mbs"));

	long startTime = System.currentTimeMillis();

	public DataRowProcessor(DynamicMarshal dynamicMarshal, List<JobEntity> jobEntities, int[] dimesions) {
		this.jobEntities = jobEntities;
		this.dynamicMarshal = dynamicMarshal;
		this.keys = dimesions;
		this.executionContext = new ExecutionContextImpl(dynamicMarshal);
		/*for(JobEntity jobEntity : jobEntities){
			Class<? extends Work> clazz = jobEntity.getJob().createNewWork().getClass();
			if(workClassTypeList.contains(clazz)) continue;
			workClassTypeList.add(clazz);
		}*/
	}

	@Override
	public void processRow(ByteBuffer row) {
		ValueSet valueSet = new ValueSet(keys);
		for (int i = 0; i < keys.length; i++) {
			Object value = dynamicMarshal.readValue(keys[i], row);
			valueSet.setValue(value, i);
		}
		freeupMemory();
		if (isNotAlreadyProcessedValueSet(valueSet)) {
			List<Work> reducers = prepareReducersBatch(valueSet);
			processReducers(reducers, row);
		}
	}

	private void freeupMemory() {
		if (totalNoOfRowsProcessed != 0 && totalNoOfRowsProcessed % MAXIMUM_NO_OF_ROWS_TO_PERFORM_GC_AFTER == 0) {
			LOGGER.info("Requesting garbage collection. No of rows processed: {}",
					new Object[] { totalNoOfRowsProcessed });
			System.gc();
			garbageCollectionRan = true;
		}
	}

	private boolean isNotAlreadyProcessedValueSet(ValueSet valueSet) {
		return processedTillValueSetInLastBatches == null || valueSet.compareTo(processedTillValueSetInLastBatches) > 0;
	}

	private List<Work> prepareReducersBatch(ValueSet valueSet) {
		checkIfBatchIsFull();
		List<Work> reducers = null;
		if (!isCurrentBatchFull) {
			reducers = addReducerWhenBatchIsNotFull(valueSet);
			additionalValueSetsPresentForProcessing = false;
		} else {
			reducers = addReducerWhenBatchIsFull(valueSet);
		}
		logProgress();
		return reducers;
	}

	private void setMaxValueSetOfCurrentBatch(ValueSet valueSet) {
		if (maxValueSetOfCurrentBatch == null || maxValueSetOfCurrentBatch.compareTo(valueSet) < 0) {
			maxValueSetOfCurrentBatch = valueSet;
		}
	}

	private void logProgress() {
		totalNoOfRowsProcessed++;
		if (totalNoOfRowsProcessed % MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER == 0) {
			LOGGER.info("Please wait... Processing in progress. {} no. of rows processed...",
					new Object[] { totalNoOfRowsProcessed });
		}
	}

	private void processReducers(List<Work> works, ByteBuffer row) {
		if (works != null) {
			executionContext.setData(row);
			for (Work work : works) {
				work.processRow(executionContext);
			}
		}
	}

	private List<Work> addReducerWhenBatchIsFull(ValueSet valueSet) {
		List<Work> reducers = valuestWorkTreeMap.get(valueSet);
		if (reducers == null && isValueSetSmallerThanMaxOfCurrentBatch(valueSet)) {
			reducers = valuestWorkTreeMap.get(maxValueSetOfCurrentBatch);
			valuestWorkTreeMap.remove(maxValueSetOfCurrentBatch);
			updateReducer(valueSet, reducers);
			additionalValueSetsPresentForProcessing = true;
		}
		if (!additionalValueSetsPresentForProcessing
				&& valueSet.compareTo(maxValueSetOfCurrentBatch) > 0) {
			additionalValueSetsPresentForProcessing = true;
		}
		return reducers;
	}

	private boolean isValueSetSmallerThanMaxOfCurrentBatch(ValueSet valueSet) {
		return maxValueSetOfCurrentBatch == null || valueSet.compareTo(maxValueSetOfCurrentBatch) <= 0;
	}

	private void addReducer(ValueSet valueSet, List<Work> works) {
		if (!valuestWorkTreeMap.containsKey(valueSet)) {
			for (JobEntity jobEntity : this.jobEntities) {
				Work work = jobEntity.getJob().createNewWork();
				works.add(work);
			}
			valuestWorkTreeMap.put(valueSet, works);
			setMaxValueSetOfCurrentBatch(valueSet);
		}
	}

	private void updateReducer(ValueSet valueSet, List<Work> reducers) {
		//int i = reducers.size();
		/*while (i > 0) {
			i--;
				for(Class<? extends Work> clazz : this.workClassTypeList){
					if (clazz != reducers.get(i).getClass()) {
						reducers.remove(i);
					}
				}
		}
		i = reducers.size();
		while (i != 1) {
			i--;
			reducers.remove(i);
		}*/
		for (Work reducer : reducers) {
			reducer.reset();
		}
		valuestWorkTreeMap.put(valueSet, reducers);
		maxValueSetOfCurrentBatch=valueSet;
		maxValueSetOfCurrentBatch = valuestWorkTreeMap.lastKey();
	}

	private List<Work> addReducerWhenBatchIsNotFull(ValueSet valueSet) {
		List<Work> works = valuestWorkTreeMap.get(valueSet);
		if (works == null) {
			works = new ArrayList<>();
			addReducer(valueSet, works);
		}
		return works;
	}

	private void checkIfBatchIsFull() {
		if (countOfRows >= NO_OF_ROWS_TO_CHECK_AVAILABLE_MEMORY_AFTER) {
			long freeMemory = MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated();
			if (!isCurrentBatchFull && freeMemory <= MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS) {
				isCurrentBatchFull = true;
			}
			countOfRows = 0;
		}
		countOfRows++;
	}

	@Override
	public void finishUp() {
		processedTillValueSetInLastBatches = maxValueSetOfCurrentBatch;
		for (Entry<ValueSet, List<Work>> e : valuestWorkTreeMap.entrySet()) {
			for (Work work : e.getValue()) {
				executionContext.setKeys(e.getKey());
				work.calculate(executionContext);
			}
		}
		if (!garbageCollectionRan) {
			System.gc();
		}
		reset();
	}

	private void reset() {
		LOGGER.info("Processing of batch id:{}, size:{} completed.",
				new Object[] { batchId, valuestWorkTreeMap.size() });
		valuestWorkTreeMap.clear();
		isCurrentBatchFull = false;
		countOfRows = 0;
		totalNoOfRowsProcessed = 0;
		maxValueSetOfCurrentBatch = null;
		batchId++;
		garbageCollectionRan = false;
		//workClassTypeList = null;
	}

	public boolean isAdditionalValueSetsPresentForProcessing() {
		return additionalValueSetsPresentForProcessing;
	}

}