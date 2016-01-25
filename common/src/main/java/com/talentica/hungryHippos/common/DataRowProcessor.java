package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
    private HashMap<ValueSet,Work> valueSetWorkMap = new HashMap<>();
    private Job job;
    private JobEntity jobEntity;
    private int[] keys;
    //reused context
    private ExecutionContextImpl executionContext;
    //private Map<Integer,Integer> jobIdRowCountMap = new HashMap<>();

    //reused valueset
    private ValueSet valueSet;

    //reused array
    private Object[] values;
    
    public DataRowProcessor(Job job){
    	this.job = job;
    	this.jobEntity = new JobEntity(job);
    }
    public DataRowProcessor(DynamicMarshal dynamicMarshal, Job job) {
        this.dynamicMarshal = dynamicMarshal;
        this.job = job;
        this.keys = job.getDimensions();
        this.values = new Object[keys.length];
		this.valueSet = new ValueSet(Property.getKeyNamesFromIndexes(keys), values);
        executionContext = new ExecutionContextImpl(dynamicMarshal);
        this.jobEntity = new JobEntity(job);
    }

    @Override
    public void processRow(ByteBuffer row) {
        for(int i=0;i<keys.length;i++){
            Object v = dynamicMarshal.readValue(keys[i],row);
            values[i] = v;
        }
        Work work = valueSetWorkMap.get(valueSet);
        if(work==null){
			ValueSet valueSet = new ValueSet(Property.getKeyNamesFromIndexes(keys),
					Arrays.copyOf(values, values.length));
            work = job.createNewWork();
            valueSetWorkMap.put(valueSet,work);
        }
        executionContext.setData(row);
        work.processRow(executionContext);
    }

    @Override
    public void finishUp() {
        for(Map.Entry<ValueSet,Work> e:valueSetWorkMap.entrySet()){
            executionContext.setKeys(e.getKey());
            e.getValue().calculate(executionContext);
        }
    }

    public void addjobEntity(JobEntity jobEntity){
    	this.jobEntity = jobEntity;
    }
    
	@Override
	public Object getJob(){
		return this.job;
	}

	@Override
	public Object getJobEntity() {
		for(ValueSet valueSet : valueSetWorkMap.keySet()){
			if(valueSetWorkMap.get(valueSet).getRowCount() == 0){
				valueSetWorkMap.remove(valueSet);
			}
			StringBuilder keyValues = new StringBuilder();
			keyValues.append("_[");
			for(Object value : valueSet.getValues()){
				if(keyValues.length() > 2){
					keyValues.append(",");
				}
				keyValues.append(value);
			}
			keyValues.append("]");
			String workerId = valueSetWorkMap.get(valueSet).getWorkerId() + keyValues;
			jobEntity.getWorkerIdRowCountMap().put(workerId, valueSetWorkMap.get(valueSet).getRowCount());
		}
		return this.jobEntity;
	}
	
	@Override
	public void processRowCount(ByteBuffer row) {
        for(int i=0;i<keys.length;i++){
            Object v = dynamicMarshal.readValue(keys[i],row);
            values[i] = v;
        }
        Work work = valueSetWorkMap.get(valueSet);
        if(work==null){
			ValueSet valueSet = new ValueSet(Property.getKeyNamesFromIndexes(keys),
					Arrays.copyOf(values, values.length));
            work = job.createNewWork();
            valueSetWorkMap.put(valueSet,work);
        }
        work.incrCountRow();
	}
}
