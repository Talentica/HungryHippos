/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.zookeeper.ZookeeperConfig;

/**
 * @author PooshanS
 *
 */
public class NodesManagerContext {

  private static NodesManager nodesManager;
  private static final Logger LOGGER = LoggerFactory.getLogger(NodesManagerContext.class);
  private static ZookeeperConfig zookeeperConfig;
  private static String ZOOKEEPER_CONFIG_FILE_PATH;

  public static NodesManager initialize(String zKconfigFilePath) throws FileNotFoundException,
      JAXBException {
    ZOOKEEPER_CONFIG_FILE_PATH = zKconfigFilePath;
    if(nodesManager == null){
      nodesManager = new NodesManager(zKconfigFilePath);
    }
    if (zookeeperConfig == null) {
      zookeeperConfig = getZookeeperConfiguration(zKconfigFilePath);
      LOGGER.info("Initialized the nodes manager.");
    }
    nodesManager.connectZookeeper(zookeeperConfig.getZookeeperServers().getServers());
    return nodesManager;
  }

  public static NodesManager getNodesManagerInstance() throws FileNotFoundException, JAXBException {
    if(ZOOKEEPER_CONFIG_FILE_PATH == null){
      LOGGER.info("Please set the zookeeper configuration xml path");
      return null;
    }
    return initialize(ZOOKEEPER_CONFIG_FILE_PATH);
  }

  public static NodesManager getNodesManagerInstance(String clientConfigFilePath)
      throws FileNotFoundException, JAXBException {
    return initialize(clientConfigFilePath);
  }

  public static ZookeeperConfig getZookeeperConfiguration(String zKconfigFilePath) {
    try {
      return JaxbUtil.unmarshalFromFile(zKconfigFilePath, ZookeeperConfig.class);
    } catch (FileNotFoundException | JAXBException e) {
      LOGGER.info("Problem occured duw to {} ", e);
    }
    return null;
  }
  
  public static void setZookeeperXmlPath(String clientConfigFilePath){
    if(ZOOKEEPER_CONFIG_FILE_PATH == null){
      NodesManagerContext.ZOOKEEPER_CONFIG_FILE_PATH = clientConfigFilePath;
    }
  }
  

}
