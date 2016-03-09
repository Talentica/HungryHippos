package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

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

	private TreeMap<ValueSet, List<Work>> valuestWorkTreeMap = new TreeMap<>();
	private List<ValueSet> valueSetList = new ArrayList<>();

	private ValueSet maxValueSetOfCurrentBatch;

	private Set<IntArrayKeyHashMap> keysList = new HashSet<>();

	private boolean isCurrentBatchFull;

	private ValueSet processedTillValueSetInLastBatches = null;

	private boolean additionalValueSetsPresentForProcessing = false;

	private Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class);

	private int batchId = 0;

	private int countOfRows = 0;

	private int totalNoOfRowsProcessed = 0;
	
	private CountDownLatch signal = null;

	public static final long MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS = Long
			.valueOf(Property.getPropertyValue("node.min.free.memory.in.mbs"));

	long startTime = System.currentTimeMillis();

	public DataRowProcessor(DynamicMarshal dynamicMarshal, List<JobEntity> jobEntities) {
		this.jobEntities = jobEntities;
		this.dynamicMarshal = dynamicMarshal;
		for(JobEntity jobEntity : jobEntities){
			IntArrayKeyHashMap keysSet = new IntArrayKeyHashMap(jobEntity.getJob().getDimensions());
			this.keysList.add(keysSet);
		}
		this.executionContext = new ExecutionContextImpl(dynamicMarshal);
	}

	@Override
	public void processRow(ByteBuffer row) {
		for (IntArrayKeyHashMap dimensions : keysList) {
			ValueSet valueSet = new ValueSet(dimensions.getValues());
			for (int i = 0; i < dimensions.getValues().length; i++) {
				Object value = dynamicMarshal.readValue(dimensions.getValues()[i], row);
				valueSet.setValue(value, i);
			}
			valueSetList.add(valueSet);
		}
		freeupMemory();
		signal = new CountDownLatch(valueSetList.size());
		for (ValueSet valueSet : valueSetList) {
			if (isNotAlreadyProcessedValueSet(valueSet)) {
				List<Work> reducers = prepareReducersBatch(valueSet);
				processReducers(reducers, row);
			}
			signal.countDown();
		}
		try {
			signal.await();
		} catch (InterruptedException e) {
			LOGGER.info("Unable to wait to complete");
		}
		valueSetList.clear();
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
			LOGGER.info("BATCH SIZE {}",valuestWorkTreeMap.size());
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
				if (Arrays.equals(valueSet.getKeyIndexes(), jobEntity.getJob()
						.getDimensions())) {
					Work work = jobEntity.getJob().createNewWork();
					works.add(work);
				}
			}
			valuestWorkTreeMap.put(valueSet, works);
			setMaxValueSetOfCurrentBatch(valueSet);
		}
	}

	private void updateReducer(ValueSet valueSet, List<Work> reducers) {
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
		LOGGER.info("Finishing up current batch id {} ...",batchId);
		processedTillValueSetInLastBatches = maxValueSetOfCurrentBatch;
		Iterator<Entry<ValueSet,List<Work>>> itr = valuestWorkTreeMap.entrySet().iterator();
		int size = valuestWorkTreeMap.size();
		while(itr.hasNext()){
			Entry<ValueSet,List<Work>> entry = itr.next();
			for(Work work : entry.getValue()) {
				executionContext.setKeys(entry.getKey());
				work.calculate(executionContext);
			}
			itr.remove();
		}
		/*for (Entry<ValueSet, List<Work>> e : valuestWorkTreeMap.entrySet()) {
			for (Work work : e.getValue()) {
				executionContext.setKeys(e.getKey());
				work.calculate(executionContext);
			}
		}*/
		reset(size);
	}

	private void reset(int size) {
		LOGGER.info("Processing of batch id:{}, size:{} completed.",
				new Object[] { batchId, size });
		valuestWorkTreeMap.clear();
		isCurrentBatchFull = false;
		countOfRows = 0;
		totalNoOfRowsProcessed = 0;
		maxValueSetOfCurrentBatch = null;
		batchId++;
		if (!garbageCollectionRan) {
			System.gc();
		}
		garbageCollectionRan = false;
	}

	public boolean isAdditionalValueSetsPresentForProcessing() {
		return additionalValueSetsPresentForProcessing;
	}

}