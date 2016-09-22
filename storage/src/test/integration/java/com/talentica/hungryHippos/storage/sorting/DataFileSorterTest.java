/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.io.File;
import java.io.IOException;

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

}
