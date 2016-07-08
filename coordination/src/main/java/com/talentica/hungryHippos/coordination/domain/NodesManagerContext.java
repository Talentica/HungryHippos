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

	private static NodesManager nodesManager = new NodesManager();
	private static final Logger LOGGER = LoggerFactory.getLogger(NodesManagerContext.class);
	private static ZookeeperConfig zookeeperConfig;
	private static String ZOOKEEPER_CONFIG_FILE = "zookeeper-config.xml"; 

	public static NodesManager initialize(String zKconfigFilePath) throws FileNotFoundException, JAXBException {
		if (zookeeperConfig == null) {
		  zookeeperConfig = JaxbUtil.unmarshalFromFile(zKconfigFilePath, ZookeeperConfig.class);
			LOGGER.info("Initialized the nodes manager.");
		}
		nodesManager.connectZookeeper(zookeeperConfig.getZookeeperServers().getServers());
		return nodesManager;
	}

 public static NodesManager getNodesManagerInstance() throws FileNotFoundException, JAXBException {
		return initialize(ZOOKEEPER_CONFIG_FILE);
	}

	public static NodesManager getNodesManagerInstance(String clientConfigFilePath)
			throws FileNotFoundException, JAXBException {
		return initialize(clientConfigFilePath);
	}

}
