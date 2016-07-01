/**
 * 
 */
package com.talentica.hungryHippos.sharding;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.sharding.utils.ShardingTableUploadService;

/**
 * @author pooshans
 *
 */
public class ShardingFileUploadTest {

  @Before
  public void setUp() throws Exception {
    String flag =
        CoordinationApplicationContext.getZkProperty().getValueByKey("cleanup.zookeeper.nodes");
    if (flag.equals("Y")) {
      CoordinationApplicationContext.getNodesManagerIntances().startup();
    }
  }

  @Test
  public void testBucketCombinationToNode() throws IOException, InterruptedException, IllegalArgumentException, IllegalAccessException {
    ShardingTableUploadService.zkUploadBucketCombinationToNodeNumbersMap();
  }
}
