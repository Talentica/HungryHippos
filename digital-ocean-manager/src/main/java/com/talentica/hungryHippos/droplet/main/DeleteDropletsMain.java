/**
 * 
 */
package com.talentica.hungryHippos.droplet.main;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class DeleteDropletsMain {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DeleteDropletsMain.class);
	public static void main(String[] args){
		validateProgramArguments(args);
		Property.initialize(PROPERTIES_NAMESPACE.NODE);
		LOGGER.info("WAITING FOR DOWNLOAD FINISH SIGNAL");
		getFinishNodeJobsSignal(CommonUtil.ZKJobNodeEnum.DOWNLOAD_FINISHED.name());
		LOGGER.info("DOWNLOAD OF OUTPUT FILE IS COMPLETED");
		LOGGER.info("DESTROYING DROPLETS");
		String deleteDropletScriptPath = Paths.get("../bin").toAbsolutePath().toString()+PathUtil.FORWARD_SLASH;
		String[] strArr = new String[] {"/bin/sh",deleteDropletScriptPath+"delete_droplet_nodes.sh",args[0]};
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("DROPLET DISTROY IS INITIATED");
		
	}
	
	/**
	 * Get download finish signal.
	 */
	private static void getFinishNodeJobsSignal(String nodeName) {
		int totalCluster = Integer.valueOf(Property.getProperties()
				.get("common.no.of.droplets").toString());
		for (int nodeId = 0; nodeId < totalCluster; nodeId++) {
			if (!getSignalFromZk(nodeId, nodeName)) {
				continue;
			}
		}
		LOGGER.info("DOWNLOADED ALL RESULTS");
	}
	
	private static void validateProgramArguments(String[] args) {
		if (args.length < 1) {
			LOGGER.info("please provide the jobuuid as argument");
			System.exit(1);
		}
	}
	
	/**
	 * Wait for finish signal from node.
	 * 
	 * @param nodeId
	 * @param finishNode
	 * @return boolean
	 */
	private static boolean getSignalFromZk(Integer nodeId, String finishNode) {
		CountDownLatch signal = new CountDownLatch(1);
		String buildPath = ZKUtils.buildNodePath(nodeId) + PathUtil.FORWARD_SLASH + finishNode;
		try {
			ZKUtils.waitForSignal(buildPath, signal);
			signal.await();
		} catch (KeeperException | InterruptedException e) {
			return false;
		}
		return true;
	}

}
