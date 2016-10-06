/**
 * 
 */
package com.talentica.hungryHippos.droplet.main;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class DeleteDropletsMain {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DeleteDropletsMain.class);
	private static String jobUUId;
	private static HungryHippoCurator curator;

	public static void main(String[] args) {
		validateProgramArguments(args);
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		// DeleteDropletsMain.nodesManager =
		// CoordinationApplicationContext.getNodesManagerIntance();
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
		String buildPath = DeleteDropletsMain.curator
				.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.DROP_DROPLETS
						.getZKJobNode());
		CountDownLatch signal = new CountDownLatch(1);
		try {
			//ZkUtils.waitForSignal(buildPath, signal); //commented by sudarshan 
			signal.await();
		} catch ( InterruptedException e) {
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
