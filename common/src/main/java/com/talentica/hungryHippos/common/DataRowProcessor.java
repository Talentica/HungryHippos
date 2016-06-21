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
import com.talentica.hungryHippos.coordination.utility.PropertyOld;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataRowProcessor implements RowProcessor {

	private static final String AFTER_GARBAGE_COLLECTION_FREE_MEMORY_AVAILABLE_IS = "After garbage collection free memory available is: ";

	private static final String BEFORE_GARBAGE_COLLECTION_FREE_MEMORY_AVAILABLE_IS = "Before garbage collection free memory available is: ";

	private static final long NO_OF_ROWS_AFTER_WHICH_TO_DO_MEMORY_CONSUMPTION_CHECK_FOR = Long
			.parseLong(PropertyOld.getPropertyValue("node.no.of.rows.to.check.memory.consumption.after").toString());

	private static final long MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER = Long
			.parseLong(PropertyOld.getPropertyValue("no.of.rows.to.log.progress.after").toString());

	private static final long NO_OF_ROWS_TO_CHECK_AVAILABLE_MEMORY_AFTER = Long
			.parseLong(PropertyOld.getPropertyValue("no.of.rows.to.check.available.memory.after").toString());

	private static final long WAIT_TIME_IN_MS_AFTER_GC_IS_REQUESTED_FOR_SINGLE_VALUEST_IN_PROCESS = Long.parseLong(
			PropertyOld.getPropertyValue("wait.time.in.ms.after.gc.is.requested.for.single.valueset.is.in.process")
					.toString());

	private static final int MAXIMUM_NO_OF_GC_RETRIES_WHEN_SINGLE_VALUEST_IS_IN_PROCESS = Integer
			.parseInt(PropertyOld.getPropertyValue("max.no.of.gc.retries.when.single.valueset.is.in.process").toString());

	private DynamicMarshal dynamicMarshal;

	private ExecutionContextImpl executionContext;

	private JobEntity jobEntity;

	private Class<? extends Work> workClassType = null;

	private TreeMap<ValueSet, List<Work>> valuesetToWorkTreeMap = new TreeMap<>();

	private ValueSet maxValueSetOfCurrentBatch;

	private int[] keys;

	private boolean isCurrentBatchFull;

	private ValueSet processedTillValueSetInLastBatches = null;

	private boolean additionalValueSetsPresentForProcessing = false;

	private Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class);

	private static int batchId = 0;

	private int retryCount = 0;

	private int countOfRows = 0;

	private int totalNoOfRowsProcessed = 0;

	public static final long MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS = Long
			.valueOf(PropertyOld.getPropertyValue("node.min.free.memory.in.mbs"));

	long startTime = System.currentTimeMillis();

	public DataRowProcessor(DynamicMarshal dynamicMarshal, JobEntity jobEntity) {
		this.jobEntity = jobEntity;
		this.dynamicMarshal = dynamicMarshal;
		this.keys = jobEntity.getJob().getDimensions();
		this.executionContext = new ExecutionContextImpl(dynamicMarshal);
		workClassType = jobEntity.getJob().createNewWork().getClass();
	}

	@Override
	public void processRow(ByteBuffer row) {
		ValueSet valueSet = new ValueSet(keys);
		for (int i = 0; i < keys.length; i++) {
			Object value = dynamicMarshal.readValue(keys[i], row);
			valueSet.setValue(value, i);
		}
		if (isNewValueSetWhichCanBeProcessed(valueSet)) {
			List<Work> reducers = prepareReducersBatch(valueSet);
			processReducers(reducers, row);
		}
	}

	private void freeupMemoryIfNeeded() {
		if (checkForFreeMemory()) {
			int size = valuesetToWorkTreeMap.size();
			if (size > 1 && isThresholdMemoryNotAvailable()) {
				logFreeMemoryAtThisPoint(BEFORE_GARBAGE_COLLECTION_FREE_MEMORY_AVAILABLE_IS);
				removeValuesToFreeupMemory();
				System.gc();
				logFreeMemoryAtThisPoint(AFTER_GARBAGE_COLLECTION_FREE_MEMORY_AVAILABLE_IS);
			} else if (size == 1 && isThresholdMemoryNotAvailable()) {
				logFreeMemoryAtThisPoint(BEFORE_GARBAGE_COLLECTION_FREE_MEMORY_AVAILABLE_IS);
				System.gc();
				waitForGarbageCollectionToBeRun();
				logFreeMemoryAtThisPoint(AFTER_GARBAGE_COLLECTION_FREE_MEMORY_AVAILABLE_IS);
			}
		}
	}

	private void logFreeMemoryAtThisPoint(String message) {
		LOGGER.info(message + " {}.",
				new Object[] { MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated(), MemoryStatus.getTotalmemory() });
	}

	private void removeValuesToFreeupMemory() {
		long usedMemory = MemoryStatus.getUsedMemory();
		long memoryNeeded = (MemoryStatus.getMaxMemory() - MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS);
		long memoryDeficiency = usedMemory - memoryNeeded;
		long numberOfValuesToBeDeleted = (long) (Math
				.ceil((double) memoryDeficiency * valuesetToWorkTreeMap.size() / usedMemory));
		LOGGER.info("Removing {} of values to free up memory.", new Object[] { numberOfValuesToBeDeleted });
		LOGGER.info("Before removing items free up memory, size of batch(valuesetToWorkTreeMap) is: {}",
				new Object[] { valuesetToWorkTreeMap.size() });
		long valuesDeletedCounter = 0;
		while (valuesDeletedCounter < numberOfValuesToBeDeleted && valuesetToWorkTreeMap.size() > 1) {
			ValueSet currentMaxValueSet = valuesetToWorkTreeMap.lastKey();
			valuesetToWorkTreeMap.remove(currentMaxValueSet);
			valuesDeletedCounter++;
		}
		maxValueSetOfCurrentBatch = valuesetToWorkTreeMap.lastKey();
		LOGGER.info(
				"After removing few items according to available memory, size of batch(valuesetToWorkTreeMap) is: {}",
				new Object[] { valuesetToWorkTreeMap.size() });
	}

	private boolean isThresholdMemoryNotAvailable() {
		return MemoryStatus
				.getMaximumFreeMemoryThatCanBeAllocated() < MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS;
	}

	private boolean checkForFreeMemory() {
		return totalNoOfRowsProcessed != 0
				&& totalNoOfRowsProcessed % NO_OF_ROWS_AFTER_WHICH_TO_DO_MEMORY_CONSUMPTION_CHECK_FOR == 0;
	}

	private void waitForGarbageCollectionToBeRun() {
		while (retryCount <= MAXIMUM_NO_OF_GC_RETRIES_WHEN_SINGLE_VALUEST_IS_IN_PROCESS) {
			LOGGER.info(
					"Waiting for GC to be run as there is a single valueset: {} for which computation is being performed and very less memory available.",
					new Object[] { valuesetToWorkTreeMap.lastKey() });
			sleep();
			retryCount++;
		}
	}

	private void sleep() {
		try {
			LOGGER.info("Sleeping for {} ms. Total no. of rows processed is: {}. ", new Object[] {
					WAIT_TIME_IN_MS_AFTER_GC_IS_REQUESTED_FOR_SINGLE_VALUEST_IN_PROCESS, totalNoOfRowsProcessed });
			Thread.sleep(WAIT_TIME_IN_MS_AFTER_GC_IS_REQUESTED_FOR_SINGLE_VALUEST_IN_PROCESS);
		} catch (InterruptedException exception) {
			throw new RuntimeException(exception);
		}
	}

	private boolean isNewValueSetWhichCanBeProcessed(ValueSet valueSet) {
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
			LOGGER.info(
					"*********  Processing in progress. {} no. of rows processed... and current batch size {} *********",
					new Object[] { totalNoOfRowsProcessed, valuesetToWorkTreeMap.size() });
			LOGGER.info(
					"Memory status (in MBs): Max free memory available-{}, Used memory-{} ,Total Memory-{}, Free memory {},Max memory-{}.",
					new Object[] { MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated(), MemoryStatus.getUsedMemory(),
							MemoryStatus.getTotalmemory(), MemoryStatus.getFreeMemory(), MemoryStatus.getMaxMemory() });
			LOGGER.info("Size of batch(valuesetToWorkTreeMap) is: {}", new Object[] { valuesetToWorkTreeMap.size() });
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
		List<Work> reducers = valuesetToWorkTreeMap.get(valueSet);
		if (reducers == null && isValueSetSmallerThanMaxOfCurrentBatch(valueSet)) {
			reducers = valuesetToWorkTreeMap.get(maxValueSetOfCurrentBatch);
			valuesetToWorkTreeMap.remove(maxValueSetOfCurrentBatch);
			maxValueSetOfCurrentBatch = valuesetToWorkTreeMap.lastKey();
			updateReducer(valueSet, reducers);
			additionalValueSetsPresentForProcessing = true;
		}
		if (!additionalValueSetsPresentForProcessing && valueSet.compareTo(maxValueSetOfCurrentBatch) > 0) {
			additionalValueSetsPresentForProcessing = true;
		}
		return reducers;
	}

	private boolean isValueSetSmallerThanMaxOfCurrentBatch(ValueSet valueSet) {
		return maxValueSetOfCurrentBatch == null || valueSet.compareTo(maxValueSetOfCurrentBatch) <= 0;
	}

	private void addReducer(ValueSet valueSet, List<Work> works) {
		if (!valuesetToWorkTreeMap.containsKey(valueSet)) {
			freeupMemoryIfNeeded();
			Work work = jobEntity.getJob().createNewWork();
			works.add(work);
			valuesetToWorkTreeMap.put(valueSet, works);
			setMaxValueSetOfCurrentBatch(valueSet);
		}
	}

	private void updateReducer(ValueSet valueSet, List<Work> reducers) {
		int i = reducers.size();
		while (i > 0) {
			i--;
			if (workClassType != reducers.get(i).getClass()) {
				reducers.remove(i);
			}
		}
		i = reducers.size();
		while (i > 1) {
			i--;
			reducers.remove(i);
		}
		for (Work reducer : reducers) {
			reducer.reset();
		}
		freeupMemoryIfNeeded();
		valuesetToWorkTreeMap.put(valueSet, reducers);
		setMaxValueSetOfCurrentBatch(valueSet);
	}

	private List<Work> addReducerWhenBatchIsNotFull(ValueSet valueSet) {
		List<Work> works = valuesetToWorkTreeMap.get(valueSet);
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
		for (Entry<ValueSet, List<Work>> e : valuesetToWorkTreeMap.entrySet()) {
			for (Work work : e.getValue()) {
				executionContext.setKeys(e.getKey());
				work.calculate(executionContext);
			}
		}
		reset();
	}

	private void reset() {
		LOGGER.info("Processing of batch id:{}, size:{} completed.",
				new Object[] { batchId, valuesetToWorkTreeMap.size() });
		valuesetToWorkTreeMap.clear();
		isCurrentBatchFull = false;
		countOfRows = 0;
		totalNoOfRowsProcessed = 0;
		maxValueSetOfCurrentBatch = null;
		batchId++;
		workClassType = null;
		retryCount = 0;
		System.gc();
	}

	public boolean isAdditionalValueSetsPresentForProcessing() {
		return additionalValueSetsPresentForProcessing;
	}

}