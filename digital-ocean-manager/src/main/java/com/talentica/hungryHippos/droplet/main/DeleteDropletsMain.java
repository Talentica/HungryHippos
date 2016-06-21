/**
 * 
 */
package com.talentica.hungryHippos.droplet.main;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.PropertyOld;
import com.talentica.hungryHippos.coordination.utility.PropertyOld.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class DeleteDropletsMain {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DeleteDropletsMain.class);
	private static String jobUUId;
	private static NodesManager nodesManager;

	public static void main(String[] args) {
		validateProgramArguments(args);
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		PropertyOld.initialize(PROPERTIES_NAMESPACE.NODE);
		DeleteDropletsMain.nodesManager = PropertyOld.getNodesManagerIntances();
		LOGGER.info("WAITING FOR THE SIGNAL OF THE DELETE DROPLET");
		waitForSignalOfDeleteDroplets();
		LOGGER.info("DROPLET DELETE SIGNAL IS RECIEVED");
		LOGGER.info("DESTROYING DROPLETS");
		String deleteDropletScriptPath = Paths.get("../bin").toAbsolutePath().toString()+PathUtil.SEPARATOR_CHAR;
		String[] strArr = new String[] {"/bin/sh",deleteDropletScriptPath+"delete_droplet_nodes.sh",args[0]};
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("DROPLET DISTROY IS INITIATED");	
	}

	private static void waitForSignalOfDeleteDroplets() {
		String buildPath = DeleteDropletsMain.nodesManager
				.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.DROP_DROPLETS
						.getZKJobNode());
		CountDownLatch signal = new CountDownLatch(1);
		try {
			ZKUtils.waitForSignal(buildPath, signal);
			signal.await();
		} catch (KeeperException | InterruptedException e) {
			LOGGER.info("Unable to wait for the signal of output zip and transfer signal");
		}

	}

	private static void validateProgramArguments(String[] args) {
		if (args.length < 1) {
			LOGGER.info("please provide  the jobuuid as first argument");
			System.exit(1);
		}
	}

}
