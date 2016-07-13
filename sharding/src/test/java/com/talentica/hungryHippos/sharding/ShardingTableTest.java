/**
 * 
 */
package com.talentica.hungryHippos.sharding;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;

/**
 * @author pooshans
 * @author sohanc
 */
public class ShardingTableTest {
  private ShardingTable shardingTable;
  private static final String basePath = "/home/pooshans/HungryHippos";
  private static final String zookeeprConfigFilePath = basePath
      + "/configuration-schema/src/main/resources/schema/zookeeper-config.xml";

  @Before
  public void setUp() throws Exception {
    //Setup test
    NodesManagerContext.setZookeeperXmlPath(zookeeprConfigFilePath);
    shardingTable = new ShardingTable();
  }

  @Test
  public void testBucketCombinationToNode() {
    try {
      shardingTable.zkUploadBucketCombinationToNodeNumbersMap();
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public void testBucketToNodeNumber() throws IllegalArgumentException, IllegalAccessException,
      IOException, InterruptedException {
    try {
      shardingTable.zkUploadBucketToNodeNumberMap();
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(true);
    }

  }

  @Test
  public void testKeyToValueToBucket() throws IllegalArgumentException, IllegalAccessException,
      IOException, InterruptedException, JAXBException {
    try {
      shardingTable.zkUploadKeyToValueToBucketMap();
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public void testBucketCombinationToNodeRead() {
    try {
      shardingTable.readBucketCombinationToNodeNumbersMap();
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public void testBucketToNodeNumberRead() {
    try {
      shardingTable.readBucketToNodeNumberMap();
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public void testKeyToValueToBucketRead() {
    try {
      shardingTable.readKeyToValueToBucketMap();
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }
}
