package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

	private HashMap<ValueSet, List<Work>> valueSetWorksMap = new HashMap<ValueSet, List<Work>>();

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
		valueSetWorksMap.clear();
	}

	public DataRowProcessor(DynamicMarshal dynamicMarshal,int[] dimensions) {
		this.dynamicMarshal = dynamicMarshal;
		this.keys = dimensions;
		this.executionContext = new ExecutionContextImpl(dynamicMarshal);
		values = new Object[keys.length];
	}
	public void putReducer(ValueSet valueSet, Work work){
		List<Work> works = this.valueSetWorksMap.get(valueSet);
		if(works == null){
			works = new ArrayList<Work>();
			this.valueSetWorksMap.put(valueSet, works);
		}
		works.add(work);
	}

	@Override
	public void processRow(ByteBuffer row) {
		for (int i = 0; i < keys.length; i++) {
			Object v = dynamicMarshal.readValue(keys[i], row);
			values[i] = v;
		}
		ValueSet valueSet = new ValueSet(keys, values);
		List<Work> works = valueSetWorksMap.get(valueSet);
		executionContext.setData(row);
		for (Work work : works) {
			if (work != null)work.processRow(executionContext);
		}
	}

	@Override
	public void finishUp() {
		for (Map.Entry<ValueSet, List<Work>> e : valueSetWorksMap.entrySet()) {
			executionContext.setKeys(e.getKey());
			for(Work work : e.getValue()){
				work.calculate(executionContext);
			}
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

}