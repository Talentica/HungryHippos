/**
 * 
 */
package com.talentica.hungryHippos.master;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.master.data.DataProvider;

public class DataPublisherStarter {

	private NodesManager nodesManager;

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DataPublisherStarter.class);
	private static DataPublisherStarter dataPublisherStarter;

	private static String jobUUId;
	public static void main(String[] args) {
		try {
			LOGGER.info("Initializing nodes manager.");
			initialize(args);
			long startTime = System.currentTimeMillis();
			broadcastSignal();
			DataProvider.publishDataToNodes(dataPublisherStarter.nodesManager);
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to for publishing.",
					((endTime - startTime) / 1000));
		} catch (Exception exception) {
			errorHandler(exception);
		}
	}

	/**
	 * @param exception
	 */
	private static void errorHandler(Exception exception) {
		LOGGER.error("Error occured while executing publishing data on nodes.",
				exception);
		try {
			ZkSignalListener.dataPublishingFailed(
					dataPublisherStarter.nodesManager,
					CommonUtil.ZKJobNodeEnum.DATA_PUBLISHING_FAILED
							.getZKJobNode());
		} catch (IOException | InterruptedException e) {
			LOGGER.info("Unable to create the node on zk.");
		}
	}

	/**
	 * @param args
	 */
	private static void initialize(String[] args) {
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		ZkSignalListener.jobuuidInBase64 = CommonUtil
				.getJobUUIdInBase64(jobUUId);
		dataPublisherStarter = new DataPublisherStarter();
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		dataPublisherStarter.nodesManager = Property.getNodesManagerIntances();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private static void broadcastSignal() throws InterruptedException,
			IOException {
		ZkSignalListener.sendSignalToNodes(dataPublisherStarter.nodesManager,
				CommonUtil.ZKJobNodeEnum.START_NODE_FOR_DATA_RECIEVER
						.getZKJobNode());
	}

}
