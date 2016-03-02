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

	private DynamicMarshal dynamicMarshal;

	private ExecutionContextImpl executionContext;

	private JobEntity jobEntity;

	private TreeMap<ValueSet, List<Work>> valuestWorkTreeMap = new TreeMap<>();

	private int[] keys;

	private boolean isCurrentBatchFull;

	private ValueSet maxValueSetOfCurrentBatch = null;

	private ValueSet processedTillValueSetInLastBatches = null;

	private boolean additionalValueSetsPresentForProcessing = false;

	private Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class);

	private int batchId = 0;

	public static final long THRESHOLD_MEMORY_IN_MBS = Long
			.valueOf(Property.getPropertyValue("node.threshold.memory.in.mbs"));

	long startTime = System.currentTimeMillis();

	public DataRowProcessor(DynamicMarshal dynamicMarshal, JobEntity jobEntity) {
		this.jobEntity = jobEntity;
		this.dynamicMarshal = dynamicMarshal;
		this.keys = jobEntity.getJob().getDimensions();
		this.executionContext = new ExecutionContextImpl(dynamicMarshal);
	}

	@Override
	public void processRow(ByteBuffer row) {
		ValueSet valueSet = new ValueSet(keys);
		for (int i = 0; i < keys.length; i++) {
			Object value = dynamicMarshal.readValue(keys[i], row);
			valueSet.setValue(value, i);
		}
		if (isNotAlreadyProcessedValueSet(valueSet)) {
			List<Work> reducers = prepareReducersBatch(valueSet);
			processReducers(reducers, row);
		}
	}

	private boolean isNotAlreadyProcessedValueSet(ValueSet valueSet) {
		return processedTillValueSetInLastBatches == null || valueSet.compareTo(processedTillValueSetInLastBatches) > 0;
	}

	private List<Work> prepareReducersBatch(ValueSet valueSet) {
		List<Work> works = null;
		checkIfBatchIsFull();
		if (!isCurrentBatchFull) {
			works = addReducerWhenBatchIsNotFull(valueSet);
			additionalValueSetsPresentForProcessing = false;
		} else if (isCurrentBatchFull) {
			works = addReducerWhenBatchIsFull(valueSet);
		}
		return works;
	}

	private void processReducers(List<Work> works, ByteBuffer row) {
		executionContext.setData(row);
		for (Work work : works) {
			work.processRow(executionContext);
		}
	}

	private List<Work> addReducerWhenBatchIsFull(ValueSet valueSet) {
		List<Work> reducers = valuestWorkTreeMap.get(valueSet);
		if (reducers == null) {
			reducers = valuestWorkTreeMap.remove(maxValueSetOfCurrentBatch);
			reducers.clear();
			addReducer(valueSet, reducers);
			maxValueSetOfCurrentBatch = valueSet;
			additionalValueSetsPresentForProcessing = true;
		}
		return reducers;
	}

	private void addReducer(ValueSet valueSet, List<Work> works) {
		if (!valuestWorkTreeMap.containsKey(valueSet)) {
			Work work = jobEntity.getJob().createNewWork();
			works.add(work);
			valuestWorkTreeMap.put(valueSet, works);
		}
	}

	private List<Work> addReducerWhenBatchIsNotFull(ValueSet valueSet) {
		List<Work> works = valuestWorkTreeMap.get(valueSet);
		if (works == null) {
			works = new ArrayList<>();
			addReducer(valueSet, new ArrayList<>());
		}
		return works;
	}

	private void checkIfBatchIsFull() {
		long freeMemory = MemoryStatus.getFreeMemory();
		if (!isCurrentBatchFull && freeMemory <= THRESHOLD_MEMORY_IN_MBS) {
			isCurrentBatchFull = true;
			maxValueSetOfCurrentBatch = valuestWorkTreeMap.lastKey();
		}
	}

	@Override
	public void finishUp() {
		processedTillValueSetInLastBatches = valuestWorkTreeMap.lastKey();
		for (Entry<ValueSet, List<Work>> e : valuestWorkTreeMap.entrySet()) {
			for (Work work : e.getValue()) {
				executionContext.setKeys(e.getKey());
				work.calculate(executionContext);
			}
		}
		reset();
	}

	private void reset() {
		LOGGER.info("Processing of batch id:{},size{} completed.", new Object[] { batchId, valuestWorkTreeMap.size() });
		valuestWorkTreeMap.clear();
		isCurrentBatchFull = false;
		batchId++;
	}

	public boolean isAdditionalValueSetsPresentForProcessing() {
		return additionalValueSetsPresentForProcessing;
	}

}