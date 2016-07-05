/**
 * 
 */
package com.talentica.hungryHippos.sharding;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.sharding.utils.ShardingTableUploadService;
import com.talentica.hungryhippos.config.client.ClientConfig;

/**
 * @author pooshans
 *
 */
public class ShardingFileUploadTest {
  private ShardingTableUploadService service;
  ClientConfig clientConfig;

  @Before
  public void setUp() throws Exception {
    service = new ShardingTableUploadService();
    String flag =
        CoordinationApplicationContext.getZkProperty().getValueByKey("cleanup.zookeeper.nodes");
    
    if (flag.equals("Y")) {
      service = new ShardingTableUploadService();
      NodesManagerContext.getNodesManagerInstance().startup();
    }
  }

  @Test
  @Ignore
  public void testBucketCombinationToNode() {
    try {
      service.zkUploadBucketCombinationToNodeNumbersMap();
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(false);
    }
  }

  @Test
  public void testBucketToNodeNumber() throws IllegalArgumentException, IllegalAccessException,
      IOException, InterruptedException {
    try {
      service.zkUploadBucketToNodeNumberMap();
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(false);
    }

  }

  @Test
  public void testKeyToValueToBucket() throws IllegalArgumentException, IllegalAccessException,
      IOException, InterruptedException {
    try {
      service.zkUploadKeyToValueToBucketMap();
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException | InterruptedException e) {
      Assert.assertFalse(false);
    }
  }
}
