package com.talentica.hungryHippos.sharding.context;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.sharding.Column;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.config.sharding.ShardingServerConfig;

/**
 * {@code ShardingApplicationContext} used for reading sharding related configuration file.
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
  private HashMap<String,Integer> keyColumnNames;


  /**
   * creates an instance of ShardinApplicationContext.
   * 
   * @param shardingFolderPath
   */
  public ShardingApplicationContext(String shardingFolderPath) {
    LOGGER.info("shardingFolderPath : " + shardingFolderPath);
    this.shardingFolderPath = shardingFolderPath;
    checkShardingFolderNull();
    try {
      shardingClientConfig =
          JaxbUtil.unmarshalFromFile(getShardingClientConfigFilePath(), ShardingClientConfig.class);
      shardingServerConfig =
          JaxbUtil.unmarshalFromFile(getShardingServerConfigFilePath(), ShardingServerConfig.class);

      getColumnsConfiguration();
    } catch (FileNotFoundException | JAXBException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  /**
   * retrieves teh sharding folder path.
   * 
   * @return
   */
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

  /**
   * retrieves the path of the sharding-client.xml file.
   * 
   * @return
   */
  public String getShardingClientConfigFilePath() {
    return getShardingFolderPath() + File.separatorChar + shardingClientConfigFileName;
  }

  /**
   * retrieves the path of the sharding-server.xml file.
   * 
   * @return
   */
  public String getShardingServerConfigFilePath() {
    return getShardingFolderPath() + File.separatorChar + shardingServerConfigFileName;
  }

  /**
   * retrieves the path of BucketToNodeNumberMap file.
   * 
   * @return
   */
  public String getBuckettoNodeNumberMapFilePath() {
    return getShardingFolderPath() + File.separatorChar + bucketToNodeNumberMapFile;
  }


  /**
   * retrieves the path to bucketCombinationToNodeNumbersMap.
   * 
   * @return
   */
  public String getBucketCombinationtoNodeNumbersMapFilePath() {
    return getShardingFolderPath() + File.separatorChar + bucketCombinationToNodeNumbersMapFile;
  }


  /**
   * retrieves the path for keyToValueBucket Map file.
   * 
   * @return
   */
  public String getKeytovaluetobucketMapFilePath() {
    return getShardingFolderPath() + File.separatorChar + keyToValueToBucketMapFile;
  }


  public ShardingClientConfig getShardingClientConfig() {
    if (shardingClientConfig == null) {
      throw new RuntimeException("ShardingApplicationContext not initialized.");
    }
    return shardingClientConfig;
  }

  /**
   * retrieves previously created instance of ShardingServerConfig. if not created one it throws
   * RuntimeException.
   * 
   * @return
   */
  public ShardingServerConfig getShardingServerConfig() {
    if (shardingServerConfig == null) {
      throw new RuntimeException("ShardingApplicationContext not initialized.");
    }
    return shardingServerConfig;
  }

  /**
   * retrieve the field details or data description of the file.
   * 
   * @return
   */
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

  /**
   * retrieve maximum file size in bytes.
   * 
   * @return
   */
  public String getMaximumShardFileSizeInBytes() {
    ShardingServerConfig shardingServerConfig = getShardingServerConfig();
    return shardingServerConfig.getMaximumShardFileSizeInBytes();

  }

  /**
   * retrieves sharding dimension.
   * 
   * @return
   */
  public String[] getShardingDimensions() {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig();
    String keyOrderString = shardingClientConfig.getShardingDimensions();
    return keyOrderString.split(",");
  }

  /**
   * retrieves column/field names.
   * 
   * @return
   */
  public HashMap<String,Integer> getColumnsConfiguration() {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig();
    List<Column> columns = shardingClientConfig.getInput().getDataDescription().getColumn();
    keyColumnNames = new HashMap<>();
    for (int index = 0; index < columns.size(); index++) {
      keyColumnNames.put(columns.get(index).getName(),index);
    }
    return keyColumnNames;
  }

  /**
   * retrieves an array of sharding indexes.
   * 
   * @return
   */
  public int[] getShardingIndexes() {
    String[] shardingKeys = getShardingDimensions();
    int[] shardingKeyIndexes = new int[shardingKeys.length];

    for (int i = 0; i < shardingKeys.length; i++) {
      shardingKeyIndexes[i] = assignShardingIndexByName(shardingKeys[i]);
    }
    return shardingKeyIndexes;
  }

  public int assignShardingIndexByName(String name) {
    if (keyColumnNames == null) {
      getColumnsConfiguration();
    }
    return keyColumnNames.get(name);
  }

  /**
   * retrieves the index.
   * 
   * @param keyId
   * @return
   */
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

  /**
   * retrieves a string, that appends fileSystem Base Path file which found from the zookeeper to
   * the distributedFilePath provided.
   * 
   * @param distributedFilePath
   * @return
   */
  public static String getShardingConfigFilePathOnZk(String distributedFilePath) {
    String fileSystemBasePath = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath();
    return fileSystemBasePath + distributedFilePath;
  }

}
