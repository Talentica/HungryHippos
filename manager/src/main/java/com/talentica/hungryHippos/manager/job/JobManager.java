package com.talentica.hungryHippos.manager.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.DataProvider;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.zookeeper.Server;
import com.talentica.hungryHippos.utility.zookeeper.ServerHeartBeat;
import com.talentica.hungryHippos.utility.zookeeper.ZKNodeFile;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

public class JobManager {

	Set<Server> regServer = null;
	static String configPath;
	static NodesManager nodesManager;
	private static Map<String, Map<Object, Node>> keyValueNodeNumberMap;
	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class.getName());

	@SuppressWarnings({ "unchecked", "resource" })
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			LOGGER.info("You have not provided external config.properties file. Default config.properties file will be use internally");
		} else if (args.length == 1) {
			Property.CONFIG_FILE = new FileInputStream(new String(args[0]));
		}
		ServerHeartBeat heartBeat = new ServerHeartBeat();
		/*String root = Property.getProperties().getProperty(
				"zookeeper.namespace_path");*/
		String tickTime = Property.getProperties().getProperty("tick.time");
		LOGGER.info("\n\tDeleting All nodes on zookeeper");
		//heartBeat.deleteAllNodes(PathUtil.FORWARD_SLASH + root);
		(nodesManager = ServerHeartBeat.init()).startup();
		ZKNodeFile serverConfigFile = new ZKNodeFile(Property.SERVER_CONF_FILE,
				Property.loadServerProperties());
		nodesManager.saveConfigFileToZNode(serverConfigFile);

		LOGGER.info("\n\tSHARDING STARTED.....");
		Sharding.doSharding();   // do the sharding
		LOGGER.info("\n\tSave the KeyValueMap configuration on zookeeper node");
		/* To save keyValueNodeNumberMap data to ZKNode */
		try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
				new FileInputStream(
						new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
								+ PathUtil.FORWARD_SLASH
								+ ZKNodeName.keyValueNodeNumberMap))) {
			keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) inKeyValueNodeNumberMap
					.readObject();
			ZKNodeFile saveKeyValueNodeNumberMap = new ZKNodeFile(
					ZKNodeName.keyValueNodeNumberMap, null,
					keyValueNodeNumberMap);
			nodesManager.saveConfigFileToZNode(saveKeyValueNodeNumberMap);

		} catch (Exception e) {
			e.printStackTrace();
		}
			LOGGER.info("\n\tPublish the data across the nodes");
			DataProvider.publishDataToNodes(nodesManager);
		
		List<Server> regServer = heartBeat.getMonitoredServers();
		LOGGER.info("\n\t\t********STARTING TO PING THE SERVER********");
		while (true) {
			for (Server server : regServer) {
				heartBeat.startPinging(server);
				Thread.sleep(Long.valueOf(tickTime));
			}
		}

	}

}
