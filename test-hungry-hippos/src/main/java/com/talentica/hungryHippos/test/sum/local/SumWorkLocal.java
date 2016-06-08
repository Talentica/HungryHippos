package com.talentica.hungryHippos.test.sum.local;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class SumWorkLocal implements Work, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5931349264723731947L;
	protected int[] dimensions;
	protected int primaryDimension;
	private float sumFor6thColumn;
	private long sumFor3rdColumn;

	public SumWorkLocal(int[] dimensions, int primaryDimension) {
		this.dimensions = dimensions;
		this.primaryDimension = primaryDimension;
	}

	@Override
	public void processRow(ExecutionContext executionContext) {
		sumFor6thColumn = sumFor6thColumn + ((Float) executionContext.getValue(6));
		sumFor3rdColumn = sumFor3rdColumn + ((Integer) executionContext.getValue(3));
	}

	@Override
	public void calculate(ExecutionContext executionContext) {
		executionContext.saveValue(6, sumFor6thColumn, "Sum");
		executionContext.saveValue(3, sumFor3rdColumn, "Sum");
	}

	@Override
	public void reset() {
		sumFor3rdColumn = 0;
		sumFor6thColumn = 0;
	}

}
