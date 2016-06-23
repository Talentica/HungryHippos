/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;

/**
 * @author PooshanS
 *
 */
public class NodesManagerContext {
  private static NodesManager nodesManager;
  private static final Logger LOGGER = LoggerFactory.getLogger(NodesManagerContext.class);
  static String zkIp = CommonUtil.getZKIp();
  
  private static NodesManager init() throws Exception {
    if (nodesManager == null) {
      nodesManager = new NodesManager();
      LOGGER.info("Initialized the nodes manager.");
    }
    nodesManager.connectZookeeper(zkIp);
    return nodesManager;
  }

  public List<Server> getMonitoredServers() throws InterruptedException {
    return nodesManager.getServers();
  }

  public static NodesManager getNodesManagerInstance() throws Exception{
    return init();
  }
}
