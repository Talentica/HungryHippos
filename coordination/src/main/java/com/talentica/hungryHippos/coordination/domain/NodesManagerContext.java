/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryhippos.config.client.CoordinationServers;

/**
 * @author PooshanS
 *
 */
public class NodesManagerContext {
	private static NodesManager nodesManager = new NodesManager();
	private static final Logger LOGGER = LoggerFactory.getLogger(NodesManagerContext.class);

	public static NodesManager initialize(CoordinationServers coordinationServers) {
		LOGGER.info("Initialized the nodes manager.");
		nodesManager.connectZookeeper(coordinationServers.getServers());
		return nodesManager;
	}

	public List<Server> getMonitoredServers() throws InterruptedException {
		return nodesManager.getServers();
	}

	public static NodesManager getNodesManagerInstance(CoordinationServers coordinationServers) {
		return initialize(coordinationServers);
	}

}
