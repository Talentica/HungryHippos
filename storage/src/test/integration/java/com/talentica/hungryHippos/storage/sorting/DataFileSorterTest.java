/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

/**
 * @author pooshans
 *
 */
public class DataFileSorterTest {

  private String inputDataDir;
  private String ouputDataDir;
  private String shardingConfDir;

  @Before
  public void setUp() {
    File file = new File("");
    inputDataDir = file.getAbsolutePath();
    ouputDataDir = file.getAbsolutePath();
    shardingConfDir = file.getAbsolutePath() + File.separatorChar + "/src/test/resources";

  }

  @Test
  public void test() {
    ProcessBuilder jobManagerProcessBuilder = new ProcessBuilder("java",
        DataFileSorter.class.getName(), inputDataDir, ouputDataDir, shardingConfDir);
    try {
      jobManagerProcessBuilder.start();
      System.out.println("Data file sorting is started...");
    } catch (IOException e) {
      System.out.println("Unable to start the data file sorter");
    }
  }
  
  @Test
  public void testDataDimension(){
      int[] dim = new int[]{0,2,5};
      int[] newDim = new int[3];
      for(int fileId = 0 ; fileId < 7 ;fileId++){
                   int startPos = 2;
                   for (int i = 0; i < dim.length; i++) {
                     newDim[i] = dim[(i + startPos) % dim.length];
                 }
                   System.out.println(Arrays.toString(newDim));
      }
  }
  
  void leftRotate(int arr[], int d) 
  {
      int i;
      for (i = 0; i < d; i++)
          leftRotatebyOne(arr);
  }

  void leftRotatebyOne(int arr[]) 
  {
      int i, temp;
      temp = arr[0];
      for (i = 0; i < arr.length - 1; i++)
          arr[i] = arr[i + 1];
      arr[i] = temp;
  }

}
