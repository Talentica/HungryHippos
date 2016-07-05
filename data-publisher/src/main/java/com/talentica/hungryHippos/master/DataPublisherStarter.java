/**
 * 
 */
package com.talentica.hungryHippos.master;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.master.context.DataPublisherApplicationContext;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.filesystem.ZookeeperFileSystem;

public class DataPublisherStarter {

	private static NodesManager nodesManager;

	private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);

	public static void main(String[] args) {
		try {
			validateArguments(args);
			ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(args[0], ClientConfig.class);
			CoordinationApplicationContext.loadAllProperty();
			CoordinationServers coordinationServers = clientConfig.getCoordinationServers();
			nodesManager = NodesManagerContext.getNodesManagerInstance();
			DataPublisherApplicationContext.inputFile = args[2];
			String isCleanUpFlagString = CoordinationApplicationContext.getZkProperty()
					.getValueByKey("cleanup.zookeeper.nodes");
			if ("Y".equals(isCleanUpFlagString)) {
				NodesManagerContext.getNodesManagerInstance().startup();
			}
			uploadCommonConfigFileToZK(coordinationServers);
			ZookeeperFileSystem fileSystem = new ZookeeperFileSystem();
			fileSystem.createFilesAsZnode(DataPublisherApplicationContext.inputFile);
			String dataParserClassName = args[1];
			DataParser dataParser = (DataParser) Class.forName(dataParserClassName)
					.getConstructor(DataDescription.class)
					.newInstance(DataPublisherApplicationContext.getConfiguredDataDescription());
			LOGGER.info("Initializing nodes manager.");
			long startTime = System.currentTimeMillis();

			DataProvider.publishDataToNodes(nodesManager, dataParser);
			sendSignalToNodes(nodesManager);
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to for publishing.", ((endTime - startTime) / 1000));
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing publishing data on nodes.", exception);
			exception.printStackTrace();
			dataPublishingFailed();
		}
	}

	private static void validateArguments(String[] args) {
		if (args.length < 3) {
			throw new RuntimeException(
					"Either missing 1st argument {coordination config file path} and/or 2nd argument {data parser class name} and/or 3rd argument {input file}.");
		}
	}

	/**
	 * 
	 */
	private static void dataPublishingFailed() {
		CountDownLatch signal = new CountDownLatch(1);
		String alertPathForDataPublisherFailure = nodesManager
				.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.DATA_PUBLISHING_FAILED.getZKJobNode());
		signal = new CountDownLatch(1);
		try {
			DataPublisherStarter.nodesManager.createPersistentNode(alertPathForDataPublisherFailure, signal);
			signal.await();
		} catch (IOException | InterruptedException e) {
			LOGGER.info("Unable to create the sharding failure path");
		}
		createErrorEncounterSignal();
	}

	private static void createErrorEncounterSignal() {
		LOGGER.info("ERROR_ENCOUNTERED signal is sent");
		String alertErrorEncounterDataPublisher = DataPublisherStarter.nodesManager
				.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.ERROR_ENCOUNTERED.getZKJobNode());
		CountDownLatch signal = new CountDownLatch(1);
		try {
			DataPublisherStarter.nodesManager.createPersistentNode(alertErrorEncounterDataPublisher, signal);
			signal.await();
		} catch (IOException | InterruptedException e) {
			LOGGER.info("Unable to create the sharding failure path");
		}
	}

	private static void sendSignalToNodes(NodesManager nodesManager) throws InterruptedException {
		CountDownLatch signal = new CountDownLatch(1);
		try {
			nodesManager.createPersistentNode(nodesManager.buildAlertPathByName(
					CommonUtil.ZKJobNodeEnum.START_NODE_FOR_DATA_RECIEVER.getZKJobNode()), signal);
		} catch (IOException e) {
			LOGGER.info("Unable to send the signal node on zk due to {}", e);
		}

		signal.await();
	}

	private static void uploadCommonConfigFileToZK(CoordinationServers coordinationServers) throws Exception {
		LOGGER.info("PUT THE CONFIG FILE TO ZK NODE");
		ZKNodeFile serverConfigFile = new ZKNodeFile(CoordinationApplicationContext.COMMON_CONF_FILE_STRING,
				CoordinationApplicationContext.getProperty().getProperties());
		NodesManagerContext.getNodesManagerInstance().saveConfigFileToZNode(serverConfigFile, null);
		LOGGER.info("Common configuration  file successfully put on zk node.");
	}

}
