package com.talentica.hungryHippos.sharding.context;

import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.sharding.Column;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.config.sharding.ShardingServerConfig;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingApplicationContext.class);

  private static ShardingClientConfig shardingClientConfig;

  private static ShardingServerConfig shardingServerConfig;
  
  public static void initialize(String shardingClientConfigPath, String shardingServerConfigPath){
    if (shardingClientConfigPath == null || shardingServerConfigPath == null) {
      LOGGER.info("Please set the sharding config File path of Zookeeper.");
      throw new RuntimeException("Please set the sharding config File path of Zookeeper.");
    }
      try {
        shardingClientConfig = JaxbUtil.unmarshalFromFile(shardingClientConfigPath, ShardingClientConfig.class);
        shardingServerConfig = JaxbUtil.unmarshalFromFile(shardingServerConfigPath, ShardingServerConfig.class);
      } catch (FileNotFoundException | JAXBException e) {
        LOGGER.error(e.toString());
        throw new RuntimeException(e);
      }
  }

  public static ShardingClientConfig getShardingClientConfig() {
    if (shardingClientConfig == null) {
      throw new RuntimeException("ShardingApplicationContext not initialized.");
    }
    return shardingClientConfig;
  }

  public static ShardingServerConfig getShardingServerConfig() {
    if (shardingServerConfig == null) {
      throw new RuntimeException("ShardingApplicationContext not initialized.");
    }
    return shardingServerConfig;
  }

  public static final FieldTypeArrayDataDescription getConfiguredDataDescription() {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig();
    FieldTypeArrayDataDescription dataDescription = null;
    List<Column> columns = shardingClientConfig.getInput().getDataDescription().getColumn();
    String[] dataTypeDescription = new String[columns.size()];
    for (int index = 0; index < columns.size(); index++) {
      String element = columns.get(index).getDataType() + "-" + columns.get(index).getSize();
      dataTypeDescription[index] = element;
    }
    dataDescription = FieldTypeArrayDataDescription.createDataDescription(dataTypeDescription,
        shardingClientConfig.getMaximumSizeOfSingleBlockData());

    return dataDescription;

  }

  public static String getMaximumShardFileSizeInBytes() {
    ShardingServerConfig shardingServerConfig = getShardingServerConfig();
    return shardingServerConfig.getMaximumShardFileSizeInBytes();

  }

  public static String[] getShardingDimensions() {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig();
    String keyOrderString = shardingClientConfig.getShardingDimensions();
    return keyOrderString.split(",");
  }

  public static String[] getColumnsConfiguration() {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig();
    List<Column> columns = shardingClientConfig.getInput().getDataDescription().getColumn();
    String[] keyColumnNames = new String[columns.size()];
    for (int index = 0; index < columns.size(); index++) {
      keyColumnNames[index] = columns.get(index).getName();
    }
    return keyColumnNames;
  }

  public static String getKeysPrefix() {
    ShardingServerConfig shardingServerConfig = getShardingServerConfig();
    return shardingServerConfig.getKeyPrefix();
  }

  public static int[] getShardingIndexes() {
    String[] shardingKeys = getShardingDimensions();
    int[] shardingKeyIndexes = new int[shardingKeys.length];
    String keysNamingPrefix = getKeysPrefix();
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
  
  public static String getShardingConfigFilePathOnZk(String distributedFilePath) {
    String fileSystemBasePath = CoordinationApplicationContext.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath();
    return fileSystemBasePath + distributedFilePath;
  }

}
