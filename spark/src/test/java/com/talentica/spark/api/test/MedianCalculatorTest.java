/**
 * 
 */
package com.talentica.spark.api.test;

import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryHippos.api.MedianCalculator;

/**
 * @author pooshans
 *
 */
public class MedianCalculatorTest {

	private static final double DELTA = 1e-15;

	@Test
	public void testSingle() {
		MedianCalculator avlTree = new MedianCalculator(1);
		Assert.assertEquals(1.0, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testTwoOnlyRight() {
		MedianCalculator avlTree = new MedianCalculator(5, 7);
		Assert.assertEquals(6.0, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testOddWithDuplicate() {
		MedianCalculator avlTree = new MedianCalculator(5, 4, 3, 2, 1, 4, 3);
		Assert.assertEquals(3, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testOddWithoutDuplicate() {
		MedianCalculator avlTree = new MedianCalculator(5, 4, 3, 2, 1);
		Assert.assertEquals(3, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testEvenWithDuplicate() {
		MedianCalculator avlTree = new MedianCalculator(5, 2, 5, 5, 2, 3);
		Assert.assertEquals(4.0, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testEvenWithoutDuplicate() {
		MedianCalculator avlTree = new MedianCalculator(10, 100, 45, 1, 8, 15);
		Assert.assertEquals(12.5, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testFirstRandom() {
		MedianCalculator avlTree = new MedianCalculator(100, 500, 65, 190, 234, 89, 1, -1, 56, 80, 100, 3);
		Assert.assertEquals(84.5, avlTree.getMedian(), DELTA);
	}

	@Test
	public void testSecondRandom() {
		MedianCalculator avlTree = new MedianCalculator(1, 2, 50, 3, 4, 30, 2, 3);
		Assert.assertEquals(3, avlTree.getMedian(), DELTA);
	}

}
