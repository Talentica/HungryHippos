package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataRowProcessor implements RowProcessor {
	private DynamicMarshal dynamicMarshal;
	private HashMap<ValueSet, Work> valueSetWorkMap = new HashMap<>();
	private HashMap<ValueSet, TaskEntity> valueSetTaskEntityMap = new HashMap<>();
	private Job job;
	private int[] keys;
	private ExecutionContextImpl executionContext;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class);

	private ValueSet valueSet;

	private Object[] values;

	public DataRowProcessor(Job job) {
		this.job = job;
	}

	public DataRowProcessor(DynamicMarshal dynamicMarshal, Job job) {
		this.dynamicMarshal = dynamicMarshal;
		this.job = job;
		this.keys = job.getDimensions();
		this.values = new Object[keys.length];
		this.valueSet = new ValueSet(Property.getKeyNamesFromIndexes(keys), values);
		executionContext = new ExecutionContextImpl(dynamicMarshal);
	}

	public DataRowProcessor(DynamicMarshal dynamicMarshal, HashMap<ValueSet, Work> valueSetWorkMap,int[] dimensions) {
		this.dynamicMarshal = dynamicMarshal;
		this.valueSetWorkMap = valueSetWorkMap;
		this.keys = dimensions;
		this.executionContext = new ExecutionContextImpl(dynamicMarshal);
	}

	@Override
	public void processRow(ByteBuffer row) {
		Object[] values = new Object[keys.length];
		for (int i = 0; i < keys.length; i++) {
			Object v = dynamicMarshal.readValue(keys[i], row);
			values[i] = v;
		}
		ValueSet valueSet = new ValueSet(Property.getKeyNamesFromIndexes(keys), Arrays.copyOf(values, values.length));
		Work work = valueSetWorkMap.get(valueSet);
			if (work != null){
				executionContext.setData(row);
				work.processRow(executionContext);
				LOGGER.info("PROCESSING FOR WORKER {}", Arrays.toString(values));
			}
	}

	@Override
	public void finishUp() {
		for (Map.Entry<ValueSet, Work> e : valueSetWorkMap.entrySet()) {
			executionContext.setKeys(e.getKey());
			e.getValue().calculate(executionContext);
		}
	}

	@Override
	public Job getJob() {
		return this.job;
	}

	@Override
	public void processRowCount(ByteBuffer row) {
		for (int i = 0; i < keys.length; i++) {
			Object v = dynamicMarshal.readValue(keys[i], row);
			values[i] = v;
		}
		TaskEntity taskEntity = valueSetTaskEntityMap.get(valueSet);
		if (taskEntity == null) {
			ValueSet valueSet = new ValueSet(Property.getKeyNamesFromIndexes(keys),
					Arrays.copyOf(values, values.length));
			Work work = job.createNewWork();
			taskEntity = new TaskEntity();
			taskEntity.setWork(work);
			taskEntity.setJob(job);
			taskEntity.setValueSet(valueSet);
			valueSetTaskEntityMap.put(valueSet, taskEntity);
		}
		taskEntity.incrRowCount();
	}

	public HashMap<ValueSet, TaskEntity> getWorkerValueSet() {
		return valueSetTaskEntityMap;
	}

	public HashMap<ValueSet, Work> getValueSetWorkMap() {
		return valueSetWorkMap;
	}

}