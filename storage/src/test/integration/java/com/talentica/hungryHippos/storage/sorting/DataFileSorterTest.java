/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author pooshans
 *
 */
public class DataFileSorterTest {

  private String inputDataDir;
  private String shardingConfDir;
  private DataFileSorter sorter;
  int[] input;
  int[] expOutput;
  int rowSize;

  @Before
  public void setUp() throws ClassNotFoundException, FileNotFoundException, KeeperException,
      InterruptedException, IOException, JAXBException {
    inputDataDir = "/home/pooshans/Desktop/data";
    shardingConfDir =
        "/home/pooshans/HungryHippos/HungryHippos/configuration-schema/src/main/resources/distribution";
    sorter = new DataFileSorter(inputDataDir, shardingConfDir);
  }

  @Test
  public void testDataFileSort() {
    try {
      sorter.doSortingDefault();
      Assert.assertTrue(true);
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException
        | JAXBException | InsufficientMemoryException e) {
      Assert.assertFalse(true);
    }
  }
}
