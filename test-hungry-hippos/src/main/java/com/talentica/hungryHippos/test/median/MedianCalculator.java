package com.talentica.hungryHippos.test.median;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public final class MedianCalculator {

	private DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

	public void addValue(Double value) {
		descriptiveStatistics.addValue(value);
	}

	public Double calculate() {
		return descriptiveStatistics.getPercentile(50);
	}

	public void clear() {
		descriptiveStatistics.clear();
	}

}
