package com.talentica.hungryHippos.accumulator.testJobs;

import com.talentica.hungryHippos.accumulator.ExecutionContext;
import com.talentica.hungryHippos.accumulator.Work;
import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;

import java.util.HashSet;

/**
 * Created by debasishc on 5/10/15.
 */
public class TestWorkUniqueCount extends TestWork{

    HashSet<CharSequence> uniqueValues = new HashSet<>();

    public TestWorkUniqueCount(int[] dimensions, int primaryDimension, int valueIndex) {
        super(dimensions,primaryDimension,valueIndex);
    }


    @Override
    public void processRow(ExecutionContext executionContext) {
        MutableCharArrayString v =  executionContext.getString(valueIndex);
        uniqueValues.add(v);

    }

    @Override
    public void calculate(ExecutionContext executionContext) {
        executionContext.saveValue(valueIndex +" : "+uniqueValues.size());
    }
}
