package com.talentica.hungryHippos.coordination.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryhippos.config.datapublisher.DatapublisherConfig;

/**
 * {@code DataPublisherApplicationContext} is used for retrieving data publisher configuration
 * file.And its properties.
 * 
 */
public class DataPublisherApplicationContext {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataPublisherApplicationContext.class);
  private static DatapublisherConfig datapublisherConfig;

  /**
   * retrieves dataPublisher configuration from zookeeper.
   * 
   * @return an instance of {@link DatapublisherConfig}
   */
  public static DatapublisherConfig getDataPublisherConfig() {
    if (datapublisherConfig == null) {
      String configurationFile =
          CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path")
              + HungryHippoCurator.ZK_PATH_SEPERATOR
              + CoordinationConfigUtil.DATA_PUBLISHER_CONFIGURATION;
      try {
        datapublisherConfig =
            (DatapublisherConfig) HungryHippoCurator.getInstance().readObject(configurationFile);
      } catch (HungryHippoException e) {
        LOGGER.error(e.getMessage());
      }
    }
    return datapublisherConfig;
  }

  /**
   * retrieves number of attempts to be made to connect to a node/machine.
   * 
   * @return an int.
   */
  public static int getNoOfAttemptsToConnectToNode() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfAttemptsToConnectToNode();
  }

  /**
   * retieves number of connection retry to be made if server is down. the time mentioned is in
   * millisecond.
   * 
   * @return an int
   */
  public static int getServersConnectRetryIntervalInMs() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getServersConnectRetryIntervalInMs();
  }

  /**
   * retrieves number of data receiver threads to run.
   * 
   * @return an int.
   */
  public static int getNoOfDataReceiverThreads() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfDataReceiverThreads();
  }

  /**
   * retrieves the number of bytes in each memory array.
   * 
   * @return an int.
   */
  public static int getNoOfBytesInEachMemoryArray() {
    if (datapublisherConfig == null) {

      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfBytesInEachMemoryArray();
  }
}
