/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;

/**
 * @author pooshans
 *
 */
public class ZkNodeSearch {

  @Before
  public void setUp() throws Exception {
    NodesManagerContext.getNodesManagerInstance();
  }

  @Test
  public void testSearchNodeByName() throws KeeperException, InterruptedException {
    List<String> nodePaths = new ArrayList<>();
    ZkUtils.getNodePathByName("/", "PUSH_JOB_NOTIFICATION", nodePaths);
    Assert.assertNotEquals(nodePaths.size(), 0);
  }
}
