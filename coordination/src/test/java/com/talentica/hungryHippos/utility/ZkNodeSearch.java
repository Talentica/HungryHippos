/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;

/**
 * @author pooshans
 *
 */
public class ZkNodeSearch {

  @Before
  public void setUp() throws Exception {
    String isCleanUpFlagString =
        CoordinationApplicationContext.getZkProperty().getValueByKey("cleanup.zookeeper.nodes");

		ObjectFactory factory = new ObjectFactory();
		CoordinationServers coordinationServers = factory.createCoordinationServers();
    if ("Y".equals(isCleanUpFlagString)) {
			NodesManagerContext.getNodesManagerInstance(coordinationServers).startup();
    }
  }

  @Test
  public void testSearchNodeByName() throws KeeperException, InterruptedException {
    List<String> nodePaths = new ArrayList<>();
    ZKUtils.getNodePathByName("/", "PUSH_JOB_NOTIFICATION", nodePaths);
    System.out.println(Arrays.toString(nodePaths.toArray()));
  }
}
