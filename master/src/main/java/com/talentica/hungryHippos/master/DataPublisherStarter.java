/**
 * 
 */
package com.talentica.hungryHippos.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.utility.CommonUtil.ZKNodeDeleteSignal;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;

public class DataPublisherStarter {

	private NodesManager nodesManager;

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);

	public static void main(String[] args) {
		try {
			Property.setNamespace(PROPERTIES_NAMESPACE.MASTER);
			DataPublisherStarter dataPublisherStarter = new DataPublisherStarter();
			LOGGER.info("Initializing nodes manager.");
			(dataPublisherStarter.nodesManager = ServerHeartBeat.init()).startup(ZKNodeDeleteSignal.MASTER.name());
			LOGGER.info("PUT THE CONFIG FILE TO ZK NODE");
			ZKNodeFile serverConfigFile = new ZKNodeFile(Property.SERVER_CONF_FILE, Property.loadServerProperties());
			dataPublisherStarter.nodesManager.saveConfigFileToZNode(serverConfigFile, null);
			LOGGER.info("CONFIG FILE PUT TO ZK NODE SUCCESSFULLY!");
			ZKNodeFile configNodeFile = new ZKNodeFile(Property.CONF_PROP_FILE + "_FILE", Property.getProperties());
			dataPublisherStarter.nodesManager.saveConfigFileToZNode(configNodeFile, null);
			DataProvider.publishDataToNodes(dataPublisherStarter.nodesManager);
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing publishing data on nodes.", exception);
		}
	}

}
