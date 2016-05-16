package com.talentica.test.youtube;
import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

public class TopVideosWork implements Work, Serializable {

	private static final long serialVersionUID = -5931349264723731947L;

	protected int[] dimensions;

	protected int primaryDimension;

	protected int valueIndex;

	private double sum;

	public TopVideosWork(int[] dimensions, int primaryDimension, int valueIndex) {
		this.dimensions = dimensions;
		this.primaryDimension = primaryDimension;
		this.valueIndex = valueIndex;
	}

	@Override
	public void processRow(ExecutionContext executionContext) {
		sum = sum + ((Double) executionContext.getValue(valueIndex));
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
