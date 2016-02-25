package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataRowProcessor implements RowProcessor {

	private DynamicMarshal dynamicMarshal;

	private ExecutionContextImpl executionContext;

	private HashMap<ValueSet, Work> valueSetWorkMap = new HashMap<>();

	private HashMap<ValueSet, TaskEntity> valueSetTaskEntityMap = new HashMap<>();

	private JobEntity jobEntity;

	private int[] keys;

	Object[] values = null;

	public DataRowProcessor(DynamicMarshal dynamicMarshal) {
		this.dynamicMarshal = dynamicMarshal;
		executionContext = new ExecutionContextImpl(dynamicMarshal);
	}

	public void setJob(JobEntity jobEntity) {
		this.jobEntity = jobEntity;
		this.keys = this.jobEntity.getJob().getDimensions();
		values = new Object[keys.length];
		valueSetTaskEntityMap.clear();
		valueSetWorkMap.clear();
	}

	public DataRowProcessor(DynamicMarshal dynamicMarshal, HashMap<ValueSet, Work> valueSetWorkMap,int[] dimensions) {
		this.dynamicMarshal = dynamicMarshal;
		this.valueSetWorkMap = valueSetWorkMap;
		this.keys = dimensions;
		this.executionContext = new ExecutionContextImpl(dynamicMarshal);
		values = new Object[keys.length];
	}

	@Override
	public void processRow(ByteBuffer row) {

		for (int i = 0; i < keys.length; i++) {
			Object v = dynamicMarshal.readValue(keys[i], row);
			values[i] = v;
		}
		ValueSet valueSet = new ValueSet(keys, values);
		Work work = valueSetWorkMap.get(valueSet);
		if (work != null){
			executionContext.setData(row);
			work.processRow(executionContext);
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
	public JobEntity getJobEntity() {
		return this.jobEntity;
	}

	@Override
	public void processRowCount(ByteBuffer row) {
		Object[] values = new Object[keys.length];
		for (int i = 0; i < keys.length; i++) {
			Object v = dynamicMarshal.readValue(keys[i], row);
			values[i] = v;
		}
		ValueSet valueSet = new ValueSet(keys, values);
		TaskEntity taskEntity = valueSetTaskEntityMap.get(valueSet);
		if (taskEntity == null) {
			Work work = this.jobEntity.getJob().createNewWork();
			taskEntity = new TaskEntity();
			taskEntity.setWork(work);
			taskEntity.setJobEntity(this.jobEntity);
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