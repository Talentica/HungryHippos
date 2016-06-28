/**
 * 
 */
package com.talentica.hungryHippos.droplet.main;

import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class DownloadOutputMain {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DownloadOutputMain.class);
	private static String jobUUId;
	public static void main(String[] args) throws FileNotFoundException{/*
		validateProgramArguments(args);
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		PropertyOld.initialize(PROPERTIES_NAMESPACE.NODE);
		NodesManager nodesManager = PropertyOld.getNodesManagerIntances();
		LOGGER.info("WAITING FOR DOWNLOAD FINISH SIGNAL");
		getFinishNodeJobsSignal(CommonUtil.ZKJobNodeEnum.DOWNLOAD_FINISHED.name());
		LOGGER.info("DOWNLOAD OF OUTPUT FILE IS COMPLETED");
		LOGGER.info("SEND SIGNAL TO OUTPUT SERVER THAT ALL FILES ARE DOWNLOAED");
		sendSignalForAllOutputFilesDownloaded(nodesManager);
		LOGGER.info("SIGNAL SENT");
		LOGGER.info("WAITING FOR THE SIGNAL OF TRANSFER AND ZIPPED FROM OUTPUT SERVER");
		waitForSignalOfOutputFileZippedAndTransferred(nodesManager);
		LOGGER.info("ALL OUTPUT FILE IS TRANSFFERED AND ZIPPED.");
	*/}
	
	/**
	 * Get download finish signal.
	 */
	private static void getFinishNodeJobsSignal(String nodeName) {/*
		int totalCluster = Integer.valueOf(PropertyOld.getProperties()
				.get("no.of.droplets").toString());
		for (int nodeId = 0; nodeId < totalCluster; nodeId++) {
			if (!getSignalFromZk(nodeId, nodeName)) {
				continue;
			}
		}
		LOGGER.info("DOWNLOADED ALL RESULTS");
	*/}
	
	private static void validateProgramArguments(String[] args) {
		if (args.length < 1) {
			LOGGER.info("please provide  the jobuuid as first argument");
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
		String buildPath = ZKUtils.buildNodePath(nodeId) + PathUtil.SEPARATOR_CHAR + finishNode;
		try {
			ZKUtils.waitForSignal(buildPath, signal);
			signal.await();
		} catch (KeeperException | InterruptedException e) {
			return false;
		}
		return true;
	}

	private static void sendSignalForAllOutputFilesDownloaded(NodesManager nodesManager){/*
		String buildPath = PropertyOld.getPropertyValue("zookeeper.base_path") + PathUtil.SEPARATOR_CHAR + CommonUtil.ZKJobNodeEnum.ALL_OUTPUT_FILES_DOWNLOADED.getZKJobNode();
		CountDownLatch signal = new CountDownLatch(1);
		try {
			nodesManager.createPersistentNode(buildPath, signal);
			signal.await();
		} catch (IOException | InterruptedException e) {
			LOGGER.info("Unable to create the path on zk node {}",buildPath);
		}
	*/}
	
	/**
	 * Await for the signal of the sharding. Once sharding is completed, it start execution for the data publishing.
	 * @param dataPublisherStarter
	 * @throws Exception
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void waitForSignalOfOutputFileZippedAndTransferred(NodesManager nodesManager){/*
		String buildPath = PropertyOld.getPropertyValue("zookeeper.base_path") + PathUtil.SEPARATOR_CHAR + CommonUtil.ZKJobNodeEnum.OUTPUT_FILES_ZIPPED_AND_TRANSFERRED.getZKJobNode();
		CountDownLatch signal = new CountDownLatch(1);
		try {
			ZKUtils.waitForSignal(buildPath, signal);
			signal.await();
		} catch (KeeperException | InterruptedException e) {
			LOGGER.info("Unable to wait for the signal of output zip and transfer signal");
		}
		
	*/}
	
	
}
