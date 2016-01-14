package com.talentica.hungryHippos.accumulator.testJobs;

import com.talentica.hungryHippos.accumulator.ExecutionContext;
import com.talentica.hungryHippos.accumulator.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class TestWork implements Work {
    protected int[] dimensions;
    protected int primaryDimension;
    protected int valueIndex;
    public int countLine = 0;
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
        executionContext.saveValue(valueIndex +" : "+value);
        //descriptiveStatistics.clear();
    }


	@Override
	public int countRow() {
		return ++countLine;	
	}
}
