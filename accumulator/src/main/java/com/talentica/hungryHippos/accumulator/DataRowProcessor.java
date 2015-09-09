package com.talentica.hungryHippos.accumulator;

import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataRowProcessor implements RowProcessor {
    private DynamicMarshal dynamicMarshal;
    private HashMap<ValueSet,Work> valueSetWorkMap = new HashMap<>();
    private Job job;
    private int[] keys;

    //reused context
    private ExecutionContextImpl executionContext;

    //reused valueset
    private ValueSet valueSet;

    //reused array
    private Object[] values;

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
            e.getValue().calculate(executionContext);
        }
    }


}
