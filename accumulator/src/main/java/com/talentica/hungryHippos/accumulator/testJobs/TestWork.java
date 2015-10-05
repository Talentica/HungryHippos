package com.talentica.hungryHippos.accumulator.testJobs;

import com.talentica.hungryHippos.accumulator.ExecutionContext;
import com.talentica.hungryHippos.accumulator.Work;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;

/**
 * Created by debasishc on 9/9/15.
 */
public class TestWork implements Work {
    protected int[] dimensions;
    protected int primaryDimension;
    protected int valueIndex;

    double value = 0;
    //DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

    public TestWork(int[] dimensions, int primaryDimension, int valueIndex) {
        this.dimensions = dimensions;
        this.primaryDimension = primaryDimension;
        this.valueIndex = valueIndex;
    }


    @Override
    public void processRow(ExecutionContext executionContext) {
        double v =  (Double)executionContext.getValue(valueIndex);
        //descriptiveStatistics.addValue(v);
        value+=v;

    }

    @Override
    public void calculate(ExecutionContext executionContext) {
        //System.out.print(Arrays.toString(dimensions)+" :: " + valueIndex + " :: ");
        executionContext.saveValue(valueIndex +" : "+value);
        //descriptiveStatistics.clear();
    }
}
