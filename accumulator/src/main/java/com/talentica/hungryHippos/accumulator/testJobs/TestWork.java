package com.talentica.hungryHippos.accumulator.testJobs;

import java.io.Serializable;

import com.talentica.hungryHippos.accumulator.ExecutionContext;
import com.talentica.hungryHippos.accumulator.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class TestWork implements Work,Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = -5931349264723731947L;
	protected int[] dimensions;
    protected int primaryDimension;
    protected int valueIndex;
    protected String workerId;
    private int countLine = 0;
    
    double value = 0;
    //DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

    public TestWork(int[] dimensions, int primaryDimension, int valueIndex, String workerId) {
        this.dimensions = dimensions;
        this.primaryDimension = primaryDimension;
        this.valueIndex = valueIndex;
        this.workerId = workerId;
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


	@Override
	public void incrCountRow() {
		countLine++;
	}


	@Override
	public int getRowCount() {
		return countLine;
	}


	@Override
	public String getWorkerId() {
		return workerId;
	}
	
	
}
