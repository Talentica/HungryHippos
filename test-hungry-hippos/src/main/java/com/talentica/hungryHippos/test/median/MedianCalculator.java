package com.talentica.hungryHippos.test.median;

import java.io.Serializable;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public final class MedianCalculator implements Serializable {

	private static final long serialVersionUID = 1L;

	private DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

	public void addValue(Double value) {
		descriptiveStatistics.addValue(value);
	}

	public void addValue(Long value) {
		descriptiveStatistics.addValue(value);
	}

	public Double calculate() {
		return descriptiveStatistics.getPercentile(50);
	}

	public void clear() {
		descriptiveStatistics.clear();
	}

}
