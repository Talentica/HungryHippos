package com.talentica.hungryHippos.test.sum.lng;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class SumWork implements Work, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5931349264723731947L;
	protected int[] dimensions;
	protected int primaryDimension;
	protected int valueIndex;
	private long sum;

	public SumWork(int[] dimensions, int primaryDimension, int valueIndex) {
		this.dimensions = dimensions;
		this.primaryDimension = primaryDimension;
		this.valueIndex = valueIndex;
	}

	@Override
	public void processRow(ExecutionContext executionContext) {
		sum = sum + ((Long) executionContext.getValue(valueIndex));
	}

	@Override
	public void calculate(ExecutionContext executionContext) {
		executionContext.saveValue(valueIndex, sum, "Sum");
	}

	@Override
	public void reset() {
		sum = 0;
	}

}
