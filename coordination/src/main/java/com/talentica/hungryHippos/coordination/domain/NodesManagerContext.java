/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;

/**
 * @author PooshanS
 *
 */
public class NodesManagerContext {
  private static NodesManager nodesManager;
  private static final Logger LOGGER = LoggerFactory.getLogger(NodesManagerContext.class);
  private static Property<ZkProperty> cProperty;
  private static String zkIp;

  private static NodesManager init() throws Exception {
    if (nodesManager == null) {
      nodesManager = new NodesManager();
      LOGGER.info("Initialized the nodes manager.");
      connectZookeeper();
    }
    return nodesManager;
  }

  public List<Server> getMonitoredServers() throws InterruptedException {
    return nodesManager.getServers();
  }

  public static NodesManager getNodesManagerInstance() throws Exception {
    return init();
  }

  private static void connectZookeeper() {
    if (cProperty == null) {
      cProperty = new ZkProperty("zookeeper.properties");
    }
    zkIp = cProperty.getValueByKey("zookeeper.server.ips");
    nodesManager.connectZookeeper(zkIp);
  }

}
