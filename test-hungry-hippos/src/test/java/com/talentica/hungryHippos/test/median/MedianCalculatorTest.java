package com.talentica.hungryHippos.test.median;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class MedianCalculatorTest {

	private MedianCalculator medianCalculator = new MedianCalculator();

	@Test
	public void testCalculateOfNumbersWithOddCount() throws IOException {
		BufferedReader bufferedReader = null;
		FileReader fileReader = null;
		try {
			fileReader = new FileReader(this.getClass().getClassLoader().getResource("medianTestOdd.txt").getPath());
			bufferedReader = new BufferedReader(fileReader);
			String lineRead = null;
			medianCalculator.clear();
			while ((lineRead = bufferedReader.readLine()) != null) {
				medianCalculator.addValue(Double.valueOf(lineRead));
			}
			double median = medianCalculator.calculate();
			Assert.assertEquals(Double.valueOf(12123), Double.valueOf(median));
		} finally {
			if (bufferedReader != null) {
				bufferedReader.close();
			}
			if (fileReader != null) {
				fileReader.close();
			}
		}
	}

	@Test
	public void testCalculateOfNumbersWithEvenCount() throws NumberFormatException, IOException {
		BufferedReader bufferedReader = null;
		FileReader fileReader = null;
		try {
			fileReader = new FileReader(this.getClass().getClassLoader().getResource("medianTestEven.txt").getPath());
			bufferedReader = new BufferedReader(fileReader);
			String lineRead = null;
			medianCalculator.clear();
			while ((lineRead = bufferedReader.readLine()) != null) {
				medianCalculator.addValue(Double.valueOf(lineRead));
			}
			double median = medianCalculator.calculate();
			Assert.assertEquals(Double.valueOf(250), Double.valueOf(median));
		} finally {
			if (bufferedReader != null) {
				bufferedReader.close();
			}
			if (fileReader != null) {
				fileReader.close();
			}
		}
	}

	@Test
	public void testCalculate() throws NumberFormatException, IOException {
		medianCalculator.clear();
		for (int i = 0; i < 100000; i++) {
			medianCalculator.addValue(Double.valueOf(i + 1));
		}
		double median = medianCalculator.calculate();
		Assert.assertEquals(Double.valueOf(50000.5), Double.valueOf(median));
	}

}
