/**
 * 
 */
package com.talentica.hungryHippos.master;

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
import com.talentica.hungryHippos.utility.ZKNodeName;

public class DataPublisherStarter {

	private NodesManager nodesManager;

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			if (args.length >= 1) {
				Property.overrideConfigurationProperties(args[0]);
			}
			DataPublisherStarter dataPublisherStarter = new DataPublisherStarter();
			Property.initialize(PROPERTIES_NAMESPACE.MASTER);
			dataPublisherStarter.nodesManager = CommonUtil.connectZK();
			LOGGER.info("Initializing nodes manager.");
			waitForSinal(dataPublisherStarter);
			DataProvider.publishDataToNodes(dataPublisherStarter.nodesManager);
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to for publishing.", ((endTime - startTime) / 1000));
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing publishing data on nodes.", exception);
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
		ZKUtils.waitForSignal(dataPublisherStarter.nodesManager.buildAlertPathByName(ZKNodeName.SHARDING_COMPLETED), signal);
		signal.await();
	}

}
