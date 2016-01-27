package com.talentica.hungryHippos.test.median;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class MedianWork implements Work, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5931349264723731947L;
	protected int[] dimensions;
	protected int primaryDimension;
	protected int valueIndex;
	protected String workerId;
	private int countLine = 0;
	private List<Double> values = new ArrayList<>();

	public MedianWork(int[] dimensions, int primaryDimension, int valueIndex, String workerId) {
		this.dimensions = dimensions;
		this.primaryDimension = primaryDimension;
		this.valueIndex = valueIndex;
		this.workerId = workerId;
	}

	@Override
	public void processRow(ExecutionContext executionContext) {
		values.add((Double) executionContext.getValue(valueIndex));
	}

	@Override
	public void calculate(ExecutionContext executionContext) {
		executionContext.saveValue(valueIndex, MedianCalculator.calculate(values));
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
