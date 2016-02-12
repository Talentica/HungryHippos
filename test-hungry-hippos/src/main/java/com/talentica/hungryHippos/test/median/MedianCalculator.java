package com.talentica.hungryHippos.test.median;

import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public final class MedianCalculator {

	public static double calculate(List<Double> values) {
		DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
		for (Double value : values) {
			descriptiveStatistics.addValue(value);
		}
		return descriptiveStatistics.getPercentile(50);
	}

}
