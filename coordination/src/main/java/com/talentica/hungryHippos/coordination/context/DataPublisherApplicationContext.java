package com.talentica.hungryHippos.coordination.context;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.datapublisher.DatapublisherConfig;

public class DataPublisherApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherApplicationContext.class);
  private static DatapublisherConfig datapublisherConfig;

  public static DatapublisherConfig getDataPublisherConfig() {
    if (datapublisherConfig == null) {
      try {
        ZKNodeFile configurationFile = (ZKNodeFile) NodesManagerContext.getNodesManagerInstance()
            .getConfigFileFromZNode(CoordinationConfigUtil.DATA_PUBLISHER_CONFIGURATION);
        datapublisherConfig = JaxbUtil.unmarshal((String) configurationFile.getObj(),DatapublisherConfig.class);
        return datapublisherConfig;
      } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
        LOGGER.info("Please upload the sharding configuration file on zookeeper");
      }catch (JAXBException e1) {
        LOGGER.info("Unable to unmarshal the coordination xml configuration.");
      }
    }
    return datapublisherConfig;
  }
  
  public static int getNoOfAttemptsToConnectToNode(){
    if(datapublisherConfig == null){
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfAttemptsToConnectToNode();
  }
  
  public static int getServersConnectRetryIntervalInMs(){
    if(datapublisherConfig == null){
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getServersConnectRetryIntervalInMs();
  }
  
  public static int getNoOfDataReceiverThreads(){
    if(datapublisherConfig == null){
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfDataReceiverThreads();
  }

  public static int getNoOfBytesInEachMemoryArray(){
    if(datapublisherConfig == null){
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfBytesInEachMemoryArray();
  }
}
