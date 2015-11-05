package com.talentica.hungryHippos.manager;

import java.io.FileInputStream;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.manager.util.PathUtil;
import com.talentica.hungryHippos.manager.zookeeper.Property;
import com.talentica.hungryHippos.manager.zookeeper.Server;
import com.talentica.hungryHippos.manager.zookeeper.ServerHeartBeat;

public class JobManager	{

	Set<Server> regServer = null;
	static String configPath;
	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class.getName());
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			LOGGER.info("You have not provided external config.properties file. Default config.properties file will be use internally");

		} else if (args.length == 1) {
			Property.CONFIG_FILE = new FileInputStream(new String(args[0]));
		}
		ServerHeartBeat heartBeat = new ServerHeartBeat();
		String root = Property.getProperties().getProperty("namespace.path");
		heartBeat.deleteAllNodes(PathUtil.SLASH+root);
		heartBeat.init().startup();
		List<Server> regServer = heartBeat.getMonitoredServers();
		LOGGER.info("\n\t\t********STARTING TO PING THE SERVER********");
		while (true) {
			for (Server server : regServer) {
				heartBeat.startPinging(server);
				Thread.sleep(5000);
			}
		}
	}
	
	

}

