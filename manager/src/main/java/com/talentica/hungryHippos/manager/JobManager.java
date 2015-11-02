package com.talentica.hungryHippos.manager;

import java.util.List;
import java.util.Set;

import com.talentica.hungryHippos.manager.util.PathUtil;
import com.talentica.hungryHippos.manager.zookeeper.Property;
import com.talentica.hungryHippos.manager.zookeeper.Server;
import com.talentica.hungryHippos.manager.zookeeper.ServerHeartBeat;

public class JobManager	{

	Set<Server> regServer = null;
	static String configPath;
	public static void main(String[] args) throws Exception {
		if(args.length == 0){
			System.out.println("Please provide config file as argument");
			return;
		}
		Property.CONFIG_PATH = args[0];
		String root = new Property().getProperties().getProperty("namespace.path");
		ServerHeartBeat heartBeat = new ServerHeartBeat();
		heartBeat.deleteAllNodes(PathUtil.SLASH+root);
		heartBeat.init().startup();
		List<Server> regServer = heartBeat.getMonitoredServers();
		System.out.println("\n\t\t********STARTING TO PING THE SERVER********");
		while (true) {
			for (Server server : regServer) {
				heartBeat.startPinging(server);
				Thread.sleep(5000);
			}
		}
	}
	
	

}

