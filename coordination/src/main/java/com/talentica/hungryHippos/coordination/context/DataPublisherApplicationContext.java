package com.talentica.hungryHippos.coordination.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryhippos.config.datapublisher.DatapublisherConfig;

public class DataPublisherApplicationContext {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataPublisherApplicationContext.class);
  private static DatapublisherConfig datapublisherConfig;

  public static DatapublisherConfig getDataPublisherConfig() {
    if (datapublisherConfig == null) {
      String configurationFile =
          CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path")
              + HungryHippoCurator.ZK_PATH_SEPERATOR + CoordinationConfigUtil.DATA_PUBLISHER_CONFIGURATION;
      try {
        datapublisherConfig = (DatapublisherConfig) HungryHippoCurator.getAlreadyInstantiated()
            .readObject(configurationFile);
      } catch (HungryHippoException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return datapublisherConfig;
  }

  public static int getNoOfAttemptsToConnectToNode() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfAttemptsToConnectToNode();
  }

  public static int getServersConnectRetryIntervalInMs() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getServersConnectRetryIntervalInMs();
  }

  public static int getNoOfDataReceiverThreads() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfDataReceiverThreads();
  }

  public static int getMaxRecordBufferSize() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getMaxRecordBufferSize();
  }
}
