package com.talentica.hungryHippos.test.median;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class MedianCalculatorTest {

	@Test
	public void testCalculateOfNumbersWithEvenCount() {
		List<Double> values = new ArrayList<>();
		values.add(12d);
		values.add(10d);
		values.add(18d);
		values.add(1d);
		double median = MedianCalculator.calculate(values);
		Assert.assertEquals(11d, median, 5);
	}

	@Test
	public void testCalculateOfNumbersWithOddCount() {
		List<Double> values = new ArrayList<>();
		values.add(12d);
		values.add(10d);
		values.add(200d);
		values.add(18d);
		values.add(1d);
		double median = MedianCalculator.calculate(values);
		Assert.assertEquals(12d, median, 5);
	}

}
