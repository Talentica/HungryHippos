package com.talentica.hungryHippos.accumulator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataRowProcessor implements RowProcessor {
    private DynamicMarshal dynamicMarshal;
    private HashMap<ValueSet,Work> valueSetWorkMap = new HashMap<>();
    private Job job;
    private int[] keys;
    private long rowCount = 0;
    //reused context
    private ExecutionContextImpl executionContext;
    //private Map<Integer,Integer> jobIdRowCountMap = new HashMap<>();

    //reused valueset
    private ValueSet valueSet;

    //reused array
    private Object[] values;
    
    public DataRowProcessor(Job job){
    	this.job = job;
    }
    public DataRowProcessor(DynamicMarshal dynamicMarshal, Job job) {
        this.dynamicMarshal = dynamicMarshal;
        this.job = job;
        this.keys = job.getDimensions();
        this.values = new Object[keys.length];
        this.valueSet = new ValueSet(values);
        executionContext = new ExecutionContextImpl(dynamicMarshal);
    }

    @Override
    public void processRow(ByteBuffer row) {
        for(int i=0;i<keys.length;i++){
            Object v = dynamicMarshal.readValue(keys[i],row);
            values[i] = v;
        }
        Work work = valueSetWorkMap.get(valueSet);
        if(work==null){
            ValueSet valueSet = new ValueSet(Arrays.copyOf(values,values.length));
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

	/*@Override
	public void rowCount(ByteBuffer row) {
        for(int i=0;i<keys.length;i++){
            Object v = dynamicMarshal.readValue(keys[i],row);
            values[i] = v;
        }
        Work work = valueSetWorkMap.get(valueSet);
        if(work==null){
            ValueSet valueSet = new ValueSet(Arrays.copyOf(values,values.length));
            work = job.createNewWork();
            valueSetWorkMap.put(valueSet,work);
        }
        work.countRow();
	}*/

	/*@Override
	public void finishRowCount() {
		int totalRows = 0;
		for(Map.Entry<ValueSet,Work> e:valueSetWorkMap.entrySet()){
            totalRows = totalRows + (e.getValue().countRow() - 1);
            Integer oldRows = jobIdRowCountMap.get(job.getJobId());
            if(oldRows == null){
            	jobIdRowCountMap.put(job.getJobId(), totalRows);
            }else{
            	jobIdRowCountMap.put(job.getJobId(), (totalRows+oldRows));
            }
        }
	}*/

	/*@Override
	public Map<Integer,Integer> getTotalRowCountByJobId() {
		return jobIdRowCountMap;
	}*/

	@Override
	public Object getJob(){
		return this.job;
	}

	@Override
	public void incrRowCount() {
		this.rowCount = this.job.incrRowCount();
	}

	@Override
	public Long totalRowCount() {
		return this.rowCount;
	}

}
