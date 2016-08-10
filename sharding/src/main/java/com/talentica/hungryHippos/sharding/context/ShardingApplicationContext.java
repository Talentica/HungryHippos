package com.talentica.hungryHippos.sharding.context;

import java.io.File;
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

  private ShardingClientConfig shardingClientConfig;

  private ShardingServerConfig shardingServerConfig;

  private String shardingFolderPath;

  public final static String shardingClientConfigFileName = "sharding-client-config.xml";
  public final static String shardingServerConfigFileName = "sharding-server-config.xml";
  public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";
  public final static String bucketCombinationToNodeNumbersMapFile =
      "bucketCombinationToNodeNumbersMap";
  public final static String keyToValueToBucketMapFile = "keyToValueToBucketMap";


  public ShardingApplicationContext(String shardingFolderPath) {
    this.shardingFolderPath = shardingFolderPath;
    checkShardingFolderNull();
    try {
      shardingClientConfig =
          JaxbUtil.unmarshalFromFile(getShardingClientConfigFilePath(), ShardingClientConfig.class);
      shardingServerConfig =
          JaxbUtil.unmarshalFromFile(getShardingServerConfigFilePath(), ShardingServerConfig.class);
    } catch (FileNotFoundException | JAXBException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }


  public String getShardingFolderPath() {
    checkShardingFolderNull();
    return shardingFolderPath;
  }


  private void checkShardingFolderNull() {
    if (shardingFolderPath == null) {
      LOGGER.info("Please set the sharding config File path of Zookeeper.");
      throw new RuntimeException("Please set the sharding config File path of Zookeeper.");
    }
  }

  public String getShardingClientConfigFilePath() {
    return getShardingFolderPath() + File.separatorChar + shardingClientConfigFileName;
  }


  public String getShardingServerConfigFilePath() {
    return getShardingFolderPath() + File.separatorChar + shardingServerConfigFileName;
  }


  public String getBuckettoNodeNumberMapFilePath() {
    return getShardingFolderPath() + File.separatorChar + bucketToNodeNumberMapFile;
  }


  public String getBucketCombinationtoNodeNumbersMapFilePath() {
    return getShardingFolderPath() + File.separatorChar + bucketCombinationToNodeNumbersMapFile;
  }


  public String getKeytovaluetobucketMapFilePath() {
    return getShardingFolderPath() + File.separatorChar + keyToValueToBucketMapFile;
  }


  public ShardingClientConfig getShardingClientConfig() {
    if (shardingClientConfig == null) {
      throw new RuntimeException("ShardingApplicationContext not initialized.");
    }
    return shardingClientConfig;
  }

  public ShardingServerConfig getShardingServerConfig() {
    if (shardingServerConfig == null) {
      throw new RuntimeException("ShardingApplicationContext not initialized.");
    }
    return shardingServerConfig;
  }

  public final FieldTypeArrayDataDescription getConfiguredDataDescription() {
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

  public String getMaximumShardFileSizeInBytes() {
    ShardingServerConfig shardingServerConfig = getShardingServerConfig();
    return shardingServerConfig.getMaximumShardFileSizeInBytes();

  }

  public String[] getShardingDimensions() {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig();
    String keyOrderString = shardingClientConfig.getShardingDimensions();
    return keyOrderString.split(",");
  }

  public String[] getColumnsConfiguration() {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig();
    List<Column> columns = shardingClientConfig.getInput().getDataDescription().getColumn();
    String[] keyColumnNames = new String[columns.size()];
    for (int index = 0; index < columns.size(); index++) {
      keyColumnNames[index] = columns.get(index).getName();
    }
    return keyColumnNames;
  }

  public String getKeysPrefix() {
    ShardingServerConfig shardingServerConfig = getShardingServerConfig();
    return shardingServerConfig.getKeyPrefix();
  }

  public int[] getShardingIndexes() {
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

  public int getShardingIndexSequence(int keyId) {
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
