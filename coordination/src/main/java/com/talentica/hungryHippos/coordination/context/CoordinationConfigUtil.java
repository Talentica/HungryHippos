/**
 * 
 */
package com.talentica.hungryHippos.coordination.context;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.utility.CoordinationProperty;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

/**
 * @author pooshans
 *
 */
public class CoordinationConfigUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinationConfigUtil.class);
  private static Property<CoordinationProperty> property;
  private static String localClusterConfigFilePath;
  private static CoordinationConfig config;
  private static ClusterConfig clusterConfig;
  public static final String COORDINATION_CONFIGURATION = "coordination-configuration";
  public static final String CLUSTER_CONFIGURATION = "cluster-configuration";
  public static final String CLIENT_CONFIGURATION = "client-configuration";
  public static final String SHARDING_CLIENT_CONFIGURATION = "sharding-client-configuration";
  public static final String SHARDING_SERVER_CONFIGURATION = "sharding-server-configuration";
  public static final String JOB_RUNNER_CONFIGURATION = "job-runner-configuration";
  public static final String DATA_PUBLISHER_CONFIGURATION = "datapublisher-configuration";
  public static final String FILE_SYSTEM = "file-system";
  public static final String TOOLS_CONFIGURATION = "tools-configuration";
  private static final String ZK_PATH_SEPERATOR = "/";
  private static HungryHippoCurator curator;

  public static Property<CoordinationProperty> getProperty() {
    if (property == null) {
      property = new CoordinationProperty("config-path.properties");
    }
    return property;
  }

  public static void uploadConfigurationOnZk(String nodeName, Object configurationFile)
      throws IOException, JAXBException, InterruptedException, HungryHippoException {
    LOGGER.info("uploading  {}  on zookeeper", nodeName);
    if (curator == null) {
      curator = HungryHippoCurator.getAlreadyInstantiated();
    }
    curator.createPersistentNode(nodeName, configurationFile);
    LOGGER.info("uploaded  {}  on zookeeper succesfully", nodeName);
  }

  public static CoordinationConfig getZkCoordinationConfigCache() {
    if (config != null) {
      return config;
    }
    String configurationFile =
        CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path") + "/"
            + CoordinationConfigUtil.COORDINATION_CONFIGURATION;
    try {
      if (curator == null) {
        curator = HungryHippoCurator.getAlreadyInstantiated();
      }
      config = (CoordinationConfig) curator.readObject(configurationFile);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return config;
  }

  public static void setLocalClusterConfigPath(String localClusterConfigFilePath) {
    CoordinationConfigUtil.localClusterConfigFilePath = localClusterConfigFilePath;
  }

  public static ClusterConfig getLocalClusterConfig() {
    ClusterConfig clusterConfig = null;
    try {
      clusterConfig = JaxbUtil.unmarshalFromFile(localClusterConfigFilePath, ClusterConfig.class);
    } catch (FileNotFoundException | JAXBException e) {
      LOGGER.info("Please provide the cluster configuration file path");
      throw new RuntimeException();
    }
    return clusterConfig;
  }

  public static ClusterConfig getZkClusterConfigCache() {
    if (clusterConfig != null) {
      return clusterConfig;
    }

    String configurationFile =
        CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path")
            + ZK_PATH_SEPERATOR + CoordinationConfigUtil.CLUSTER_CONFIGURATION;
    try {
      if (curator == null) {
        curator = HungryHippoCurator.getAlreadyInstantiated();
      }
      clusterConfig = (ClusterConfig) curator.readObject(configurationFile);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return clusterConfig;

  }

}
