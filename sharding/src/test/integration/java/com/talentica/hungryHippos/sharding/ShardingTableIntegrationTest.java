/**
 * 
 */
package com.talentica.hungryHippos.sharding;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;

/**
 * @author pooshans
 * @author sohanc
 */
public class ShardingTableIntegrationTest {
  private ShardingTableZkService shardingTable;
  private static final String shardingPath = "/dir/dir1/input";

  @Before
  public void setUp() throws Exception {
    shardingTable = new ShardingTableZkService();
  }

  @Test
  @Ignore
  public void testBucketCombinationToNode() {
    try {
      shardingTable.zkUploadBucketCombinationToNodeNumbersMap(shardingPath);
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  @Ignore
  public void testBucketToNodeNumber() throws IllegalArgumentException, IllegalAccessException,
      IOException, InterruptedException {
    try {
      shardingTable.zkUploadBucketToNodeNumberMap(shardingPath);
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  @Ignore
  public void testKeyToValueToBucket() throws IllegalArgumentException, IllegalAccessException,
      IOException, InterruptedException, JAXBException {
    try {
      shardingTable.zkUploadKeyToValueToBucketMap(shardingPath);
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  @Ignore
  public void testBucketCombinationToNodeRead() {
    try {
      shardingTable.readBucketCombinationToNodeNumbersMap(shardingPath);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  @Ignore
  public void testBucketToNodeNumberRead() {
    try {
      shardingTable.readBucketToNodeNumberMap(shardingPath);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public void testKeyToValueToBucketRead() {
    try {
      shardingTable.readKeyToValueToBucketMap(shardingPath);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public void testBucketToNodeNumberObject() throws Exception {
    try {
      ZkUtils.saveObjectZkNode("/rootnode/test",
          shardingTable.getKeyToValueToBucketMap());
      Assert.assertTrue(true);
    } catch (IllegalArgumentException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public <V, K> void testGetBucketToNodeNumberObject() throws Exception {
      Object object  = ZkUtils.readObjectZkNode("/rootnode/test");
      Assert.assertNotNull(object);
 }
}
