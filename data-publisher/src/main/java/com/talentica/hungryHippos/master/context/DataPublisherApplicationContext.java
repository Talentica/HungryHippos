package com.talentica.hungryHippos.master.context;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.datapublisher.DatapublisherConfig;

public class DataPublisherApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherApplicationContext.class);
  private static DatapublisherConfig datapublisherConfig;

  public static DatapublisherConfig getDataPublisherConfig(NodesManager manager) {
    if (datapublisherConfig == null) {
      try {
        ZKNodeFile configurationFile = (ZKNodeFile) manager
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
}
