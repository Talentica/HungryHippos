/**
 * 
 */
package com.talentica.hungryHippos.coordination.context;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.utility.CoordinationProperty;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

/**
 * @author pooshans
 *
 */
public class CoordinationApplicationContext {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CoordinationApplicationContext.class);
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

  public static Property<CoordinationProperty> getProperty() {
    if (property == null) {
      property = new CoordinationProperty("config-path.properties");
    }
    return property;
  }

  public static void uploadConfigurationOnZk(NodesManager manager, String nodeName,
      String configurationFile) throws IOException, JAXBException, InterruptedException {
    LOGGER.info("Updating coordination configuration on zookeeper");
    ZKNodeFile configFile = new ZKNodeFile(nodeName, configurationFile);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    manager.saveConfigFileToZNode(configFile, countDownLatch);
    countDownLatch.await();
  }

  public static CoordinationConfig getZkCoordinationConfigCache() {
    if (config != null) {
      return config;
    }
    try {
      ZKNodeFile configurationFile = (ZKNodeFile) NodesManagerContext.getNodesManagerInstance()
          .getObjectFromZKNode(getProperty().getValueByKey("zookeeper.config_path")
              + ZkUtils.zkPathSeparator + CoordinationApplicationContext.COORDINATION_CONFIGURATION);
      config = JaxbUtil.unmarshal((String) configurationFile.getObj(), CoordinationConfig.class);
      return config;
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
      LOGGER.info("Please upload the sharding configuration file on zookeeper");
    } catch (JAXBException e1) {
      LOGGER.info("Unable to unmarshal the coordination xml configuration.");
    }
    return config;
  }

  public static void setLocalClusterConfigPath(String localClusterConfigFilePath) {
    CoordinationApplicationContext.localClusterConfigFilePath = localClusterConfigFilePath;
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
    try {
      ZKNodeFile configurationFile = (ZKNodeFile) NodesManagerContext.getNodesManagerInstance()
          .getConfigFileFromZNode(CoordinationApplicationContext.CLUSTER_CONFIGURATION);
      clusterConfig = JaxbUtil.unmarshal((String) configurationFile.getObj(), ClusterConfig.class);
      return clusterConfig;
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
      LOGGER.info("Please upload the sharding configuration file on zookeeper");
    } catch (JAXBException e1) {
      LOGGER.info("Unable to unmarshal the coordination xml configuration.");
    }
    return clusterConfig;
  }

}
