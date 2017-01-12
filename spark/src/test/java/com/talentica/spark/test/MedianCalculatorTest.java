/**
 * 
 */
package com.talentica.spark.test;

import org.apache.commons.lang.IllegalClassException;
import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryHippos.utility.MedianCalculator;

/**
 * @author pooshans
 *
 */
public class MedianCalculatorTest {

  private static final double DELTA = 1e-15;

  @Test
  public void testSingle() {
    MedianCalculator<Integer> medianCalculator = new MedianCalculator<Integer>(new Integer[] {1});
    Assert.assertEquals(1.0, medianCalculator.median(), DELTA);
  }

  @Test
  public void testTwoOnly() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {5, 7});
    Assert.assertEquals(6.0, medianCalculator.median(), DELTA);
  }

  @Test
  public void testOddWithDuplicate() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {5, 4, 3, 2, 1, 4, 3});
    Assert.assertEquals(3, medianCalculator.median(), DELTA);
  }

  @Test
  public void testOddWithoutDuplicate() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {5, 4, 3, 2, 1});
    Assert.assertEquals(3, medianCalculator.median(), DELTA);
  }

  @Test
  public void testEvenWithDuplicate() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {5, 2, 5, 5, 2, 3});
    Assert.assertEquals(4.0, medianCalculator.median(), DELTA);
  }

  @Test
  public void testEvenWithoutDuplicate() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {10, 100, 45, 1, 8, 15});
    Assert.assertEquals(12.5, medianCalculator.median(), DELTA);
  }

  @Test
  public void testFirstRandom() {
    MedianCalculator<Integer> medianCalculator = new MedianCalculator<Integer>(
        new Integer[] {100, 500, 65, 190, 234, 89, 1, -1, 56, 80, 100, 3});
    Assert.assertEquals(84.5, medianCalculator.median(), DELTA);
  }

  @Test
  public void testSecondRandom() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {1, 2, 50, 3, 4, 30, 2, 3});
    Assert.assertEquals(3, medianCalculator.median(), DELTA);
  }

  @Test
  public void testThirdRandom() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {-1, -2, -50, -3, -4, -30, -2, -3, -2, 10});
    Assert.assertEquals(-2.5, medianCalculator.median(), DELTA);
  }

  @Test
  public void testFourthRandom() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {100, 300, 200, 500, 100, 400, 600, 499, 240});
    Assert.assertEquals(300, medianCalculator.median(), DELTA);
  }

  @Test
  public void testFifthRandom() {
    MedianCalculator<Integer> medianCalculator =
        new MedianCalculator<Integer>(new Integer[] {5, 29, 10, 33, 33, 33, 6, 7, 8, 10});
    Assert.assertEquals(10, medianCalculator.median(), DELTA);
  }

  @Test
  public void testSixthRandom() {
    MedianCalculator<Double> medianCalculator = new MedianCalculator<Double>(
        new Double[] {5.0, 29.4, 5.67, 10.99, 33.34, 33.90, 33.38, 6.00, 7.24, 8.88});
    Assert.assertEquals(9.935, medianCalculator.median(), DELTA);
  }

  @Test
  public void testDataType() {
    try {
      MedianCalculator<String> medianCalculator =
          new MedianCalculator<String>(new String[] {"a", "b"});
      medianCalculator.median();
    } catch (IllegalClassException icex) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testFifthWithAddSequencially() {
    int[] keys = new int[] {100, 300, 200, 500, 100, 400, 600, 499, 240};
    MedianCalculator<Integer> medianCalculator = new MedianCalculator<Integer>();
    for (int key : keys) {
      medianCalculator.add(key);
    }
    Assert.assertEquals(300, medianCalculator.median(), DELTA);
  }
}
