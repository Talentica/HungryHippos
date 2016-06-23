/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;

/**
 * @author pooshans
 *
 */
public class CoordinationApplicationContext {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CoordinationApplicationContext.class);

  private static Property<CoordinationProperty> property;
  private static Property<ZkProperty> zkProperty;
  private static Property<ServerProperty> serverProperty;
  private static NodesManager nodesManager;
  private static FieldTypeArrayDataDescription dataDescription;
  public static final String SERVER_CONF_FILE = "serverConfigFile.properties";

  public static final String SERVER_CONFIGURATION_KEY_PREFIX = "server.";

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
  
  public static Property<ServerProperty> getServerProperty(){
    if (serverProperty == null) {
      serverProperty = new ServerProperty("server-config.properties");
    }
    return serverProperty;
  }

  public static final FieldTypeArrayDataDescription getConfiguredDataDescription() {
    if (property == null) {
      getProperty();
    }
    if (dataDescription == null) {
      dataDescription =
          FieldTypeArrayDataDescription.createDataDescription(
              ((CoordinationProperty) property).getDataTypeConfiguration(),
              ((CoordinationProperty) property).getMaximumSizeOfSingleDataBlock());
    }
    return dataDescription;
  }

  /*public static Properties loadServerProperties() {
    if (serverProperty == null) {
      serverProperty = new ServerProperty();
      try {
        InputStream is =
            new FileInputStream(CommonUtil.TEMP_JOBUUID_FOLDER_PATH + SERVER_CONF_FILE);
        serverProperty.load(is);
        PropertyConfigurator.configure(serverProperty);
        LOGGER.info("serverConfigFile.properties file is loaded");
      } catch (IOException e) {
        LOGGER.warn("Unable to load serverConfigFile.properties file");
      }
    }
    return serverProp;
  }*/

  public static String[] getShardingDimensions() {
    if (property == null) {
      getProperty();
    }
    String keyOrderString = property.getValueByKey("common.sharding_dimensions").toString();
    return keyOrderString.split(",");
  }

  public static int[] getShardingIndexes() {
    if (property == null) {
      getProperty();
    }
    String keyOrderString = property.getValueByKey("common.sharding_dimensions").toString();
    String[] shardingKeys = keyOrderString.split(",");
    int[] shardingKeyIndexes = new int[shardingKeys.length];
    String keysNamingPrefix = property.getValueByKey("common.keys.prefix");
    int keysNamingPrefixLength = keysNamingPrefix.length();
    for (int i = 0; i < shardingKeys.length; i++) {
      shardingKeyIndexes[i] =
          Integer.parseInt(shardingKeys[i].substring(keysNamingPrefixLength)) - 1;
    }
    return shardingKeyIndexes;
  }

  public static int getShardingIndexSequence(int keyId) {
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

  public static String[] getKeyNamesFromIndexes(int[] keyIndexes) {
    String[] keyColumnNames = getColumnsConfiguration();
    String[] result = new String[keyIndexes.length];
    for (int i = 0; i < keyIndexes.length; i++) {
      result[i] = keyColumnNames[keyIndexes[i]];
    }
    return result;
  }

  public static String[] getColumnsConfiguration() {
    String[] keyColumnNames = property.getValueByKey("common.column.names").toString().split(",");
    return keyColumnNames;
  }

  public static final String[] getDataTypeConfiguration() {
    if (property == null) {
      getProperty();
    }
    return property.getValueByKey("column.datatype-size").toString().split(",");
  }

  public static final int getMaximumSizeOfSingleDataBlock() {
    if (property == null) {
      getProperty();
    }
    return Integer.parseInt(property.getValueByKey("maximum.size.of.single.block.data"));
  }

  public static NodesManager getNodesManagerIntances() {
    try {
      nodesManager = NodesManagerContext.getNodesManagerInstance();
    } catch (Exception e) {
      LOGGER.warn("Unable to connect to zookeeper");
    }
    return nodesManager;
  }

  public static int getTotalNumberOfNodes() {
    int totalNumberOfNodes = 0;
    for (Object key : getServerProperty().getProperties().keySet()) {
      if (StringUtils.startsWith(key.toString(), SERVER_CONFIGURATION_KEY_PREFIX)) {
        totalNumberOfNodes++;
      }
    }
    return totalNumberOfNodes;
  }
}