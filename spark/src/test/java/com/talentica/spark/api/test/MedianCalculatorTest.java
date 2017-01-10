/**
 * 
 */
package com.talentica.spark.api.test;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.api.MedianCalculator;

/**
 * @author pooshans
 *
 */
public class MedianCalculatorTest {

	private static final double DELTA = 1e-15;

	@Test
	@Ignore
	public void testSingle() {
		MedianCalculator avlTree = new MedianCalculator(1);
		Assert.assertEquals(1.0, avlTree.getMedian(), DELTA);
	}

	@Test
	@Ignore
	public void testTwoOnlyRight() {
		MedianCalculator avlTree = new MedianCalculator(5, 7);
		Assert.assertEquals(6.0, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testOdd() {
		int[] a = new int[] { 5, 2, 9, 1, 3, 7, 12, 8, 11, 13 };
		Arrays.sort(a);
		System.out.println(Arrays.toString(a));
		MedianCalculator avlTree = new MedianCalculator(5, 2, 9, 1, 3, 7, 12, 8, 11, 13);
		Assert.assertEquals(7.5, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testOdd1() {
		MedianCalculator avlTree = new MedianCalculator(5, 4, 3, 2, 1);
		Assert.assertEquals(3, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testEven() {
		MedianCalculator avlTree = new MedianCalculator(5, 2, 5, 5, 2, 3);
		Assert.assertEquals(4.0, avlTree.getMedian(), DELTA);
	}

}
