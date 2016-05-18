/**
 * 
 */
package com.talentica.hungryHippos.master;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.master.data.DataProvider;

public class DataPublisherStarter {

	private NodesManager nodesManager;

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);
	private static DataPublisherStarter dataPublisherStarter;
	public static void main(String[] args) {
		try {
			String jobUUId = args[0];
			CommonUtil.loadDefaultPath(jobUUId);
			dataPublisherStarter = new DataPublisherStarter();
			Property.initialize(PROPERTIES_NAMESPACE.MASTER);
			dataPublisherStarter.nodesManager = Property.getNodesManagerIntances();
			LOGGER.info("Initializing nodes manager.");
			waitForSinal(dataPublisherStarter);
			long startTime = System.currentTimeMillis();
			DataProvider.publishDataToNodes(dataPublisherStarter.nodesManager);
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to for publishing.", ((endTime - startTime) / 1000));
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing publishing data on nodes.", exception);
			dataPublishingFailed();
		}
	}

	/**
	 * 
	 */
	private static void dataPublishingFailed() {
		CountDownLatch signal = new CountDownLatch(1);
		String alertPathForDataPublisherFailure = dataPublisherStarter.nodesManager
				.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.DATA_PUBLISHING_FAILED.getZKJobNode());
		signal = new CountDownLatch(1);
		try {
			dataPublisherStarter.nodesManager.createPersistentNode(alertPathForDataPublisherFailure, signal);
			signal.await();
		} catch (IOException | InterruptedException e) {
			LOGGER.info("Unable to create the sharding failure path");
		}
		createErrorEncounterSignal();
	}

	/**
	 * 
	 */
	private static void createErrorEncounterSignal() {
		LOGGER.info("ERROR_ENCOUNTERED signal is sent");
		String alertErrorEncounterDataPublisher = dataPublisherStarter.nodesManager
				.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.ERROR_ENCOUNTERED
						.getZKJobNode());
				CountDownLatch signal = new CountDownLatch(1);
				try {
					dataPublisherStarter.nodesManager.createPersistentNode(alertErrorEncounterDataPublisher, signal);
					signal.await();
				} catch (IOException | InterruptedException e) {
					LOGGER.info("Unable to create the sharding failure path");
				}
	}

	/**
	 * Await for the signal of the sharding. Once sharding is completed, it start execution for the data publishing.
	 * @param dataPublisherStarter
	 * @throws Exception
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void waitForSinal(DataPublisherStarter dataPublisherStarter)
			throws Exception, KeeperException, InterruptedException {
		CountDownLatch signal = new CountDownLatch(1);
		ZKUtils.waitForSignal(dataPublisherStarter.nodesManager.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.SHARDING_COMPLETED.getZKJobNode()), signal);
		signal.await();
	}

}
