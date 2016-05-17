/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;

/**
 * @author PooshanS
 *
 */
public class ZkSignalListener {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ZkSignalListener.class);

	public static void waitForDownloadSinal(NodesManager nodesManager,
			String nodeName) throws Exception, KeeperException,
			InterruptedException {
		listenerOnAlertNode(nodesManager, nodeName);
	}

	public static void waitForSamplingSinal(NodesManager nodesManager,
			String nodeName) throws Exception, KeeperException,
			InterruptedException {
		listenerOnAlertNode(nodesManager, nodeName);
	}

	/**
	 * Send signal to data publisher node that sharding is completed.
	 * 
	 * @param nodesManager
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void sendSignal(NodesManager nodesManager, String nodeName)
			throws IOException, InterruptedException {
		createOnAlertNode(nodesManager, nodeName);
	}

	/**
	 * @param exception
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void shardingFailed(NodesManager nodesManager, String nodeName)
			throws IOException, InterruptedException {
		createOnAlertNode(nodesManager, nodeName);
		createErrorEncounterSignal(nodesManager,
				CommonUtil.ZKJobNodeEnum.ERROR_ENCOUNTERED.getZKJobNode());
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	private static void createErrorEncounterSignal(NodesManager nodesManager,
			String nodeName) throws IOException, InterruptedException {
		createOnAlertNode(nodesManager, nodeName);
		LOGGER.info("ERROR_ENCOUNTERED node is created");
	}

	/**
	 * @param nodesManager
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void listenerOnAlertNode(NodesManager nodesManager,
			String nodeName) throws KeeperException, InterruptedException {
		CountDownLatch signal = new CountDownLatch(1);
		ZKUtils.waitForSignal(nodesManager.buildAlertPathByName(nodeName),
				signal);
		signal.await();
	}

	private static void createOnAlertNode(NodesManager nodesManager,
			String nodeName) throws IOException, InterruptedException {
		String shardingNodeName = nodesManager.buildAlertPathByName(nodeName);
		CountDownLatch signal = new CountDownLatch(1);
		nodesManager.createPersistentNode(shardingNodeName, signal);
		signal.await();
	}

}
