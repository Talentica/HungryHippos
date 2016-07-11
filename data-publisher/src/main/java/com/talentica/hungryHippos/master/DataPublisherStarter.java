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
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.filesystem.ZookeeperFileSystem;

public class DataPublisherStarter {

  private static NodesManager nodesManager;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);

  public static void main(String[] args) {
    try {
      validateArguments(args);
      CoordinationApplicationContext.setCoordinationConfigPathContext(args[0]);
      NodesManagerContext.setZookeeperXmlPath(args[1]);
      ShardingApplicationContext.setShardingConfigPathContext(args[2]);
      nodesManager = NodesManagerContext.getNodesManagerInstance();
      ZookeeperFileSystem fileSystem = new ZookeeperFileSystem();
      fileSystem.createFilesAsZnode(CoordinationApplicationContext.getCoordinationConfig()
          .getInputFileConfig().getInputFileName());
      String dataParserClassName = ShardingApplicationContext.getShardingConfig().getInput()
          .getDataParserConfig().getClassName();
      DataParser dataParser =
          (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
              .newInstance(CoordinationApplicationContext.getConfiguredDataDescription());
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
          "Either missing 1st argument {coordination config file path} and/or 2nd argument {zookeeper xml path} and/or 3rd argument {sharding xml path}.");
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

  /*
   * private static void uploadCommonConfigFileToZK(CoordinationServers coordinationServers) throws
   * Exception { LOGGER.info("PUT THE CONFIG FILE TO ZK NODE"); ZKNodeFile serverConfigFile = new
   * ZKNodeFile(CoordinationApplicationContext.COMMON_CONF_FILE_STRING,
   * CoordinationApplicationContext.getProperty().getProperties());
   * NodesManagerContext.getNodesManagerInstance().saveConfigFileToZNode(serverConfigFile, null);
   * LOGGER.info("Common configuration  file successfully put on zk node."); }
   */

}
