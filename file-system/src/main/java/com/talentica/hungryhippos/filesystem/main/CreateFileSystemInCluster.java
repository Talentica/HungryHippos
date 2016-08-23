package com.talentica.hungryhippos.filesystem.main;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.ExecuteShellCommand;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;

public class CreateFileSystemInCluster {

	private static final String SCRIPT_LOC = "/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/file-system-commands.sh";

	public static void main(String[] args) throws FileNotFoundException, JAXBException {
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

		// remove from zookeeper first.
		boolean flag = false;
		if (operation.contains("delete")) {
			runHungryHipposFileSystemMain(clientConfig, operation, fname);
			flag = true;
		}

		if (operation.equals("ls")) {
			runHungryHipposFileSystemMain(clientConfig, operation, fname);
			System.exit(1);
		}

		for (Node node : nodesInCluster) { // don't execute ls on node
			argumentsTobePassed.add(node.getIp());
			scriptArgs = argumentsTobePassed.stream().toArray(String[]::new);
			errorCount = ExecuteShellCommand.executeScript(scriptArgs);
			argumentsTobePassed.remove(node.getIp());

		}

		if (errorCount == 0 && !flag) {
			runHungryHipposFileSystemMain(clientConfig, operation, fname);

		}

	}

	private static void validateArguments(String... args) {
		if (args.length < 5) {
			throw new IllegalArgumentException(
					"Need client , Cluster Configuration details , file Operations and location");
		}
	}

	private static void runHungryHipposFileSystemMain(String clientConfig, String operation, String fname)
			throws FileNotFoundException, JAXBException {
		NodesManagerContext.getNodesManagerInstance(clientConfig);
		HungryHipposFileSystemMain.getHHFSInstance();
		HungryHipposFileSystemMain.getCommandDetails(operation, fname);
	}

}
