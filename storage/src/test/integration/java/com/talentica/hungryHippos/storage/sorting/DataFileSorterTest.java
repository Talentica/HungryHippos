/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

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

  @Before
  public void setUp() {
    inputDataDir = "/home/pooshans/Desktop/data";
    shardingConfDir =
        "/home/pooshans/HungryHippos/HungryHippos/configuration-schema/src/main/resources/distribution";

  }

  @Test
  public void testDataFileSort() {
    DataFileSorter sorter;
    try {
      sorter = new DataFileSorter(inputDataDir, shardingConfDir);
      sorter.doSortingDefault();
      Assert.assertTrue(true);
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException
        | JAXBException | InsufficientMemoryException e) {
      Assert.assertFalse(true);
    }

  }
}
