/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author pooshans
 *
 */
public class DataFileHeapSortTest {

  private int[] actualInput = new int[]{5, 3, 6, 4, 8, 9 , 1, 10};
  private int[] expectedOutput = new int[] {1,3,4,5,6,8,9,10};
  
  @Test
  public void testMerge() {
      /*DataFileHeapSort heapSort = new DataFileHeapSort(actualInput);
      heapSort.heapSort();
      Assert.assertArrayEquals(expectedOutput, actualInput);*/
  }
}
