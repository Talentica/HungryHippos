package com.talentica.hungryhippos.filesystem.main;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.utility.ExecuteShellCommand;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;

public class CreateFileSystemInCluster {

	private static final String SCRIPT_LOC = "/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/file-system-commands.sh";
	
	public static void main(String[] args) {
		validateArguments(args);
		List<String> argumentsTobePassed = new ArrayList<String>();
		String clientConfig = args[0];
		String clusterConfig = args[1];
		String userName = args[2];
		String operation = args[3];
		String fname = args[4];
		argumentsTobePassed.add("/bin/sh");
		argumentsTobePassed.add(SCRIPT_LOC);
		argumentsTobePassed.add(userName);
		argumentsTobePassed.add(operation);
		argumentsTobePassed.add(fname);
		CoordinationApplicationContext.setLocalClusterConfigPath(clusterConfig);
		ClusterConfig configuration = CoordinationApplicationContext.getLocalClusterConfig();
		int errorCount = 0;
		String[] scriptArgs = null;
		List<Node> nodesInCluster = configuration.getNode();
		for (Node node : nodesInCluster) {
			argumentsTobePassed.add(node.getIp());
			scriptArgs = argumentsTobePassed.stream().toArray(String[]::new);
			errorCount = ExecuteShellCommand.executeScript(scriptArgs);
			argumentsTobePassed.remove(node.getIp());
		}

		if (errorCount == 0) {
			HungryHipposFileSystemMain.getHHFSInstance(clientConfig);
			HungryHipposFileSystemMain.getCommandDetails(operation, fname);
		} else {
			throw new RuntimeException("Something went wrong");
		}

	}

	private static void validateArguments(String... args) {
		if (args.length < 5) {
			throw new IllegalArgumentException("Need client , Cluster Configuration details , file Operations and location");
		}
	}

}
