/**
 * 
 */
package com.talentica.hungryHippos.coordination.context;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;
import com.talentica.hungryHippos.coordination.utility.CoordinationProperty;
import com.talentica.hungryHippos.coordination.utility.ServerProperty;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
import com.talentica.hungryhippos.config.coordination.Node;

/**
 * @author pooshans
 *
 */
public class CoordinationApplicationContext {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CoordinationApplicationContext.class);

  private static Property<CoordinationProperty> property;
  private static Property<ZkProperty> zkProperty;
  private static Property<ServerProperty> serverProperty;
  private static FieldTypeArrayDataDescription dataDescription;
  private static String coordinationConfigFilePath;
  private static CoordinationConfig config;

  public static final String SERVER_CONF_FILE = "serverConfigFile.properties";

  public static final String CLUSTER_CONFIGURATION = "cluster-configuration";

  public static final String COMMON_CONF_FILE_STRING = "common-config.properties";



  public CoordinationApplicationContext(CoordinationServers coordinationServers) {

  }

  public static Property<CoordinationProperty> getProperty() {
    if (property == null) {
      property = new CoordinationProperty("coordination-config.properties");
    }
    return property;
  }

  public static Property<ZkProperty> getZkProperty() {
    if (zkProperty == null) {
      zkProperty = new ZkProperty("zookeeper.properties");
    }
    return zkProperty;
  }

  public static Property<ServerProperty> getServerProperty() {
    if (serverProperty == null) {
      serverProperty = new ServerProperty("server-config.properties");
    }
    return serverProperty;
  }

  public static final FieldTypeArrayDataDescription getConfiguredDataDescription()
      throws ClassNotFoundException, FileNotFoundException, KeeperException, InterruptedException,
      IOException, JAXBException {
    if (config == null) {
      config = getZkCoordinationConfig();
    }
    if (dataDescription == null) {
      dataDescription = FieldTypeArrayDataDescription.createDataDescription(
          config.getInputFileConfig().getColumnDatatypeSize().split(","),
          config.getCommonConfig().getMaximumSizeOfSingleBlockData());
    }
    return dataDescription;
  }

  public static String[] getShardingDimensions() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    if (config == null) {
      config = getZkCoordinationConfig();
    }
    String keyOrderString = config.getCommonConfig().getShardingDimensions();
    return keyOrderString.split(",");
  }

  public static int[] getShardingIndexes() throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException {
    if (config == null) {
      config = getZkCoordinationConfig();
    }
    String keyOrderString = config.getCommonConfig().getShardingDimensions();
    String[] shardingKeys = keyOrderString.split(",");
    int[] shardingKeyIndexes = new int[shardingKeys.length];
    String keysNamingPrefix = config.getCommonConfig().getKeysPrefix();
    int keysNamingPrefixLength = keysNamingPrefix.length();
    for (int i = 0; i < shardingKeys.length; i++) {
      shardingKeyIndexes[i] =
          Integer.parseInt(shardingKeys[i].substring(keysNamingPrefixLength)) - 1;
    }
    return shardingKeyIndexes;
  }

  public static int getShardingIndexSequence(int keyId) throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    int[] shardingIndexes = getShardingIndexes();
    int index = -1;
    for (int i = 0; i < shardingIndexes.length; i++) {
      if (shardingIndexes[i] == keyId) {
        index = i;
        break;
      }
    }
    return index;
  }

  public static String[] getKeyNamesFromIndexes(int[] keyIndexes) throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    String[] keyColumnNames = getColumnsConfiguration();
    String[] result = new String[keyIndexes.length];
    for (int i = 0; i < keyIndexes.length; i++) {
      result[i] = keyColumnNames[keyIndexes[i]];
    }
    return result;
  }

  public static String[] getColumnsConfiguration() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    if (config == null) {
      config = getZkCoordinationConfig();
    }
    String[] keyColumnNames = config.getInputFileConfig().getColumnNames().split(",");
    return keyColumnNames;
  }

  public static final String[] getDataTypeConfiguration() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    if (config == null) {
      config = getZkCoordinationConfig();
    }
    return config.getInputFileConfig().getColumnDatatypeSize().split(",");
  }

  public static final int getMaximumSizeOfSingleDataBlock() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    if (config == null) {
      config = getZkCoordinationConfig();
    }
    return config.getCommonConfig().getMaximumSizeOfSingleBlockData();
  }

  public static void loadAllProperty() {
    getProperty();
    getZkProperty();
    getServerProperty();
  }

  public static void updateClusterConfiguration(String clusterConfigurationFile)
      throws IOException, JAXBException, InterruptedException {
    LOGGER.info("Updating cluster configuration on zookeeper");
    ZKNodeFile serverConfigFile = new ZKNodeFile(CLUSTER_CONFIGURATION, clusterConfigurationFile);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    NodesManagerContext.getNodesManagerInstance().saveConfigFileToZNode(serverConfigFile,
        countDownLatch);
    countDownLatch.await();
  }

  public static CoordinationConfig getZkCoordinationConfig() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    ZKNodeFile clusterConfigurationFile = (ZKNodeFile) NodesManagerContext.getNodesManagerInstance()
        .getConfigFileFromZNode(CoordinationApplicationContext.CLUSTER_CONFIGURATION);
    CoordinationConfig coordinationConfig =
        JaxbUtil.unmarshal((String) clusterConfigurationFile.getObj(), CoordinationConfig.class);
    return coordinationConfig;
  }

  public static List<Node> getServers() throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException {
    CoordinationConfig coordinationConfig = getCoordinationConfig();
    return coordinationConfig.getClusterConfig().getNode();
  }

  public static CoordinationConfig getCoordinationConfig()
      throws FileNotFoundException, JAXBException {
    if (CoordinationApplicationContext.coordinationConfigFilePath == null) {
      LOGGER.info("Please set the coordination configuration file path.");
      return null;
    }
    return JaxbUtil.unmarshalFromFile(CoordinationApplicationContext.coordinationConfigFilePath,
        CoordinationConfig.class);
  }

  public static void setCoordinationConfigPathContext(String coordinationConfigFilePath) {
    CoordinationApplicationContext.coordinationConfigFilePath = coordinationConfigFilePath;
  }
}
