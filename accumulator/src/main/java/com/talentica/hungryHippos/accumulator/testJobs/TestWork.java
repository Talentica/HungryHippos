package com.talentica.hungryHippos.accumulator.testJobs;

import com.talentica.hungryHippos.accumulator.ExecutionContext;
import com.talentica.hungryHippos.accumulator.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class TestWork implements Work {
    private int[] dimensions;
    private int primaryDimension;
    private int valueIndex;

    double value = 0;

    public TestWork(int[] dimensions, int primaryDimension, int valueIndex) {
        this.dimensions = dimensions;
        this.primaryDimension = primaryDimension;
        this.valueIndex = valueIndex;
    }


    @Override
    public void processRow(ExecutionContext executionContext) {
        double v =  (Double)executionContext.getValue(valueIndex);
        System.out.println(executionContext.getValue(0));
        System.out.println(executionContext.getValue(1));
        System.out.println(executionContext.getValue(2));
        System.out.println(executionContext.getValue(3));
        System.out.println(executionContext.getValue(4));
        System.out.println(executionContext.getValue(5));
        System.out.println(executionContext.getValue(6));
        System.out.println(executionContext.getValue(7));
        System.out.println(executionContext.getValue(8));

        value+=v;

    }

    @Override
    public void calculate(ExecutionContext executionContext) {
        executionContext.saveValue(value);
    }
}
