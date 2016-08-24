/**
 *
 */
package com.talentica.hungryHippos.coordination.domain;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

/**
 * @author PooshanS
 *
 */
public class NodesManagerContext {

  private static NodesManager nodesManager;
  private static ClientConfig clientConfig;
  private static final Logger LOGGER = LoggerFactory.getLogger(NodesManagerContext.class);

  public static NodesManager initialize(String configFilePath)
      throws FileNotFoundException, JAXBException {
    if(nodesManager == null){
      nodesManager = new NodesManager();
      if (clientConfig == null) {
        clientConfig = JaxbUtil.unmarshalFromFile(configFilePath, ClientConfig.class);
        LOGGER.info("Initialized the nodes manager.");
      }
      nodesManager.connectZookeeper(clientConfig.getCoordinationServers().getServers(),
          Integer.parseInt(clientConfig.getSessionTimout()));
    }

    return nodesManager;
  }

  public static NodesManager getNodesManagerInstance() throws FileNotFoundException, JAXBException {
    if(nodesManager == null){
      LOGGER.error("Nodes Manager not initialized");
      System.exit(1);
    }
    return nodesManager;
  }

  public static NodesManager getNodesManagerInstance(String clientConfigFilePath)
          throws FileNotFoundException, JAXBException {
    initialize(clientConfigFilePath);
    CoordinationConfig coordinationConfig = CoordinationConfigUtil.getZkCoordinationConfigCache();
    nodesManager.initializeZookeeperDefaultConfig(coordinationConfig.getZookeeperDefaultConfig());
    return nodesManager;
  }

  public static ClientConfig getClientConfig(){
    return clientConfig;
  }

}
