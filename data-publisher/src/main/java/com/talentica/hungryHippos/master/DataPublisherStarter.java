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
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.master.util.DataPublisherApplicationContext;

public class DataPublisherStarter {

  private static NodesManager nodesManager;

  /**
   * @param args
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);
  private static DataPublisherStarter dataPublisherStarter;

  public static void main(String[] args) {
    try {
      validateArguments(args);
      String jobUUId = args[0];
      CommonUtil.loadDefaultPath(jobUUId);
      DataPublisherApplicationContext.getProperty();
      String dataParserClassName = args[1];
      DataParser dataParser =
          (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
              .newInstance(DataPublisherApplicationContext.getConfiguredDataDescription());
      //dataPublisherStarter = new DataPublisherStarter();
      String zkIp = CommonUtil.getZKIp();
      nodesManager = NodesManagerContext.getNodesManagerInstance().connectZookeeper(zkIp);
      DataPublisherApplicationContext.setNodesManager(nodesManager);
      LOGGER.info("Initializing nodes manager.");
      long startTime = System.currentTimeMillis();
      DataProvider.publishDataToNodes(nodesManager, dataParser);
      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to for publishing.", ((endTime - startTime) / 1000));
    } catch (Exception exception) {
      LOGGER.error("Error occured while executing publishing data on nodes.", exception);
      exception.printStackTrace();
      dataPublishingFailed();
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 2) {
      throw new RuntimeException("Missing job uuid and/or data parser class name parameters.");
    }
  }

  /**
	 * 
	 */
  private static void dataPublishingFailed() {
    CountDownLatch signal = new CountDownLatch(1);
    String alertPathForDataPublisherFailure =
        nodesManager
            .buildAlertPathByName(CommonUtil.ZKJobNodeEnum.DATA_PUBLISHING_FAILED.getZKJobNode());
    signal = new CountDownLatch(1);
    try {
      dataPublisherStarter.nodesManager.createPersistentNode(alertPathForDataPublisherFailure,
          signal);
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
    String alertErrorEncounterDataPublisher =
        dataPublisherStarter.nodesManager
            .buildAlertPathByName(CommonUtil.ZKJobNodeEnum.ERROR_ENCOUNTERED.getZKJobNode());
    CountDownLatch signal = new CountDownLatch(1);
    try {
      dataPublisherStarter.nodesManager.createPersistentNode(alertErrorEncounterDataPublisher,
          signal);
      signal.await();
    } catch (IOException | InterruptedException e) {
      LOGGER.info("Unable to create the sharding failure path");
    }
  }

}
