/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
/**
 * 
 */
package com.talentica.spark.test;

import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryhippos.ds.DescriptiveStatisticsNumber;

/**
 * @author pooshans
 *
 */
public class DescriptiveStatisticsNumberTest {

  private static final double DELTA = 1e-15;

  @Test
  public void testSingle() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(new Integer[] {1});
    Assert.assertEquals(1.0, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testTwoOnly() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(new Integer[] {5, 7});
    Assert.assertEquals(6.0, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testOddWithDuplicate() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(new Integer[] {5, 4, 3, 2, 1, 4, 3});
    Assert.assertEquals(3, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testOddWithoutDuplicate() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(new Integer[] {5, 4, 3, 2, 1});
    Assert.assertEquals(3, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testEvenWithDuplicate() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(new Integer[] {5, 2, 5, 5, 2, 3});
    Assert.assertEquals(4.0, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testEvenWithoutDuplicate() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(new Integer[] {10, 100, 45, 1, 8, 15});
    Assert.assertEquals(12.5, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testFirstRandom() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(
            new Integer[] {100, 500, 65, 190, 234, 89, 1, -1, 56, 80, 100, 3});
    Assert.assertEquals(84.5, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testSecondRandom() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(new Integer[] {1, 2, 50, 3, 4, 30, 2, 3});
    Assert.assertEquals(3, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testThirdRandom() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(
            new Integer[] {-1, -2, -50, -3, -4, -30, -2, -3, -2, 10});
    Assert.assertEquals(-2.5, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testFourthRandom() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(
            new Integer[] {100, 300, 200, 500, 100, 400, 600, 499, 240});
    Assert.assertEquals(300, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testFifthRandom() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(
            new Integer[] {5, 29, 10, 33, 33, 33, 6, 7, 8, 10});
    Assert.assertEquals(10, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testSixthRandom() {
    DescriptiveStatisticsNumber<Double> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Double>(
            new Double[] {5.0, 29.4, 5.67, 10.99, 33.34, 33.90, 33.38, 6.00, 7.24, 8.88});
    Assert.assertEquals(9.935, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testFifthWithAddSequencially() {
    Integer[] keys = new Integer[] {100, 300, 200, 500, 100, 400, 600, 499, 240};
    DescriptiveStatisticsNumber<Double> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Double>();
    for (int key : keys) {
      descriptiveStatistics.add(key);
    }
    Assert.assertEquals(300, (double) descriptiveStatistics.percentile(50), DELTA);
  }

  @Test
  public void testUnique() {
    DescriptiveStatisticsNumber<Integer> descriptiveStatistics =
        new DescriptiveStatisticsNumber<Integer>(
            new Integer[] {5, 29, 5, 33, 33, 33, 6, 10, 7, 8, 10});
    Assert.assertEquals(7, descriptiveStatistics.unique(), DELTA);
  }

}
