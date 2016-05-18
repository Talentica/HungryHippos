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
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * This class is listener on the zookeeper node and also creates the nodes to
 * broadcast the signal in distributed system.
 * 
 * @author PooshanS
 * @since 2016-05-18
 * @version 0.6.0
 */
public class ZkSignalListener {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ZkSignalListener.class);
	public static String jobuuidInBase64;

	/**
	 * @param nodesManager
	 * @param nodeName
	 * @throws Exception
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void waitForDownloadSinal(NodesManager nodesManager,
			String nodeName) throws Exception, KeeperException,
			InterruptedException {
		listenerOnAlertNode(nodesManager, nodeName);
	}

	/**
	 * @param nodesManager
	 * @param nodeName
	 * @throws Exception
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
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
		createErrorEncounterSignal(nodesManager);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public static void createErrorEncounterSignal(NodesManager nodesManager)
			throws IOException, InterruptedException {
		createOnAlertNode(nodesManager,
				CommonUtil.ZKJobNodeEnum.ERROR_ENCOUNTERED.getZKJobNode());
		LOGGER.info("ERROR_ENCOUNTERED node is created");
	}

	/**
	 * Await for the signal of the sharding. Once sharding is completed, it
	 * start execution for the data publishing.
	 * 
	 * @param dataPublisherStarter
	 * @throws Exception
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void waitForSignal(NodesManager nodesManager, String nodeName)
			throws Exception, KeeperException, InterruptedException {
		listenerOnAlertNode(nodesManager, nodeName);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public static void dataPublishingFailed(NodesManager nodesManager,
			String nodeName) throws IOException, InterruptedException {
		createOnAlertNode(nodesManager, nodeName);
		createErrorEncounterSignal(nodesManager);
	}

	public static void sendSignalToNodes(NodesManager nodesManager,
			String nodeName) throws InterruptedException, IOException {
		createOnAlertNode(nodesManager, nodeName);
	}

	/**
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void waitForStartDataReciever(NodesManager nodesManager,
			String nodeName) throws KeeperException, InterruptedException {
		listenerOnAlertNode(nodesManager, nodeName);
	}

	/**
	 * Await for the data publishing to be completed. Once completed, it start
	 * the execution of the job manager.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void waitForCompletion(NodesManager nodesManager,
			String nodeName) throws KeeperException, InterruptedException {
		listenerOnAlertNode(nodesManager, nodeName);
	}

	/**
	 * @param nodesManager
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void listenerOnAlertNode(NodesManager nodesManager,
			String nodeName) throws KeeperException, InterruptedException {
		CountDownLatch signal = new CountDownLatch(1);
		ZKUtils.waitForSignal(nodesManager.buildAlertPathByName(jobuuidInBase64 + PathUtil.FORWARD_SLASH + nodeName),
				signal);
		signal.await();
	}

	/**
	 * @param nodesManager
	 * @param nodeName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void createOnAlertNode(NodesManager nodesManager,
			String nodeName) throws IOException, InterruptedException {
		String shardingNodeName = nodesManager.buildAlertPathByName(jobuuidInBase64 + PathUtil.FORWARD_SLASH + nodeName);
		CountDownLatch signal = new CountDownLatch(1);
		nodesManager.createPersistentNode(shardingNodeName, signal);
		signal.await();
	}

}
