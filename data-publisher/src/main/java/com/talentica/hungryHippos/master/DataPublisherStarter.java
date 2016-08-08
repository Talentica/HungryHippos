/**
 * 
 */
package com.talentica.hungryHippos.master;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

public class DataPublisherStarter {

  private static NodesManager nodesManager;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);

  public static void main(String[] args) {

      validateArguments(args);
      String clientConfigFilePath = args[0];
      String sourcePath = args[1];
      String destinationPath = args[2];
      try {
        nodesManager = NodesManagerContext.getNodesManagerInstance(clientConfigFilePath);
      CoordinationConfig coordinationConfig =
          CoordinationApplicationContext.getZkCoordinationConfigCache();
      nodesManager.initializeZookeeperDefaultConfig(coordinationConfig.getZookeeperDefaultConfig());
      String dataParserClassName = ShardingApplicationContext
          .getShardingClientConfig(destinationPath).getInput().getDataParserConfig().getClassName();
      DataParser dataParser = (DataParser) Class.forName(dataParserClassName)
          .getConstructor(DataDescription.class)
          .newInstance(ShardingApplicationContext.getConfiguredDataDescription(destinationPath));
      LOGGER.info("Initializing nodes manager.");
      long startTime = System.currentTimeMillis();
      DataProvider.publishDataToNodes(nodesManager, dataParser, sourcePath, destinationPath);
        updateFilePublishSuccessful(destinationPath);

      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to for publishing.", ((endTime - startTime) / 1000));
    } catch (Exception exception) {
      updateFilePublishFailure(destinationPath);
      LOGGER.error("Error occured while executing publishing data on nodes.", exception);
      exception.printStackTrace();
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 3) {
      throw new RuntimeException(
          "Either missing 1st argument {client configuration} or 2nd argument {source path} or 3rd argument {destination path}");
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
      DataPublisherStarter.nodesManager.createPersistentNode(alertPathForDataPublisherFailure,
          signal);
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
      DataPublisherStarter.nodesManager.createPersistentNode(alertErrorEncounterDataPublisher,
          signal);
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

  /**
   * Updates file published successfully
   * @param destinationPath
     */
  private static void updateFilePublishSuccessful(String destinationPath) {
    String destinationPathNode = CoordinationApplicationContext.getZkCoordinationConfigCache().
            getZookeeperDefaultConfig().getFilesystemPath() + destinationPath;
    String pathForSuccessNode = destinationPathNode + "/" + FileSystemConstants.DATA_READY;
    String pathForFailureNode = destinationPathNode + "/" + FileSystemConstants.PUBLISH_FAILED;
    ZkUtils.deleteZKNode(pathForFailureNode);
    ZkUtils.createZKNodeIfNotPresent(pathForSuccessNode, "");
  }

  /**
   * Updates file pubising failed
   * @param destinationPath
     */
  private static void updateFilePublishFailure(String destinationPath) {
    String destinationPathNode = CoordinationApplicationContext.getZkCoordinationConfigCache().
            getZookeeperDefaultConfig().getFilesystemPath() + destinationPath;
    String pathForSuccessNode = destinationPathNode + "/" + FileSystemConstants.DATA_READY;
    String pathForFailureNode = destinationPathNode + "/" + FileSystemConstants.PUBLISH_FAILED;
    ZkUtils.deleteZKNode(pathForSuccessNode);
    ZkUtils.createZKNodeIfNotPresent(pathForFailureNode, "");
  }

}
