/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;

/**
 * @author PooshanS
 *
 */
public class NodesManagerContext {

	private static NodesManager nodesManager = new NodesManager();
	private static final Logger LOGGER = LoggerFactory.getLogger(NodesManagerContext.class);
	private static ClientConfig clientConfig;
	private static String CLIENT_CONFIG_FILE = "client-config.xml"; 

	public static NodesManager initialize(String configFilePath) throws FileNotFoundException, JAXBException {
		if (clientConfig == null) {
			clientConfig = JaxbUtil.unmarshalFromFile(configFilePath, ClientConfig.class);
			LOGGER.info("Initialized the nodes manager.");
		}
		nodesManager.connectZookeeper(clientConfig.getCoordinationServers().getServers());
		return nodesManager;
	}

	public List<Server> getMonitoredServers() throws InterruptedException {
		return nodesManager.getServers();
	}

	public static NodesManager getNodesManagerInstance() throws FileNotFoundException, JAXBException {
		return initialize(CLIENT_CONFIG_FILE);
	}

	public static NodesManager getNodesManagerInstance(String clientConfigFilePath)
			throws FileNotFoundException, JAXBException {
		return initialize(clientConfigFilePath);
	}

}
