package com.talentica.hungryHippos.sharding.context;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
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

  private static Map<String, ShardingClientConfig> shardingClientConfigCache = new HashMap<>();

  private static Map<String, ShardingServerConfig> shardingServerConfigCache = new HashMap<>();

  public static ShardingClientConfig getShardingClientConfig(String path) {
    if (path == null) {
      LOGGER.info("Please set the sharding config File path of Zookeeper.");
      return null;
    }
    if (!shardingClientConfigCache.containsKey(path)) {
      try {
        ZKNodeFile configFile = (ZKNodeFile) NodesManagerContext.getNodesManagerInstance()
            .getObjectFromZKNode(getShardingConfigFilePathOnZk(path) + File.separatorChar
                + CoordinationApplicationContext.SHARDING_CLIENT_CONFIGURATION);
        ShardingClientConfig shardingClientConfig =
            JaxbUtil.unmarshal((String) configFile.getObj(), ShardingClientConfig.class);
        shardingClientConfigCache.put(path, shardingClientConfig);
      } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
        LOGGER.info("Please upload the sharding configuration file on zookeeper");
        throw new RuntimeException(e);
      } catch (JAXBException e1) {
        LOGGER.info("Unable to unmarshal the sharding xml configuration.");
        throw new RuntimeException(e1);
      }
    }
    return shardingClientConfigCache.get(path);
  }

  public static ShardingServerConfig getShardingServerConfig(String path) {
    if (path == null) {
      LOGGER.info("Please set the sharding config File path of Zookeeper.");
      return null;
    }
    if (!shardingServerConfigCache.containsKey(path)) {
      try {
        ZKNodeFile configFile = (ZKNodeFile) NodesManagerContext.getNodesManagerInstance()
            .getObjectFromZKNode(getShardingConfigFilePathOnZk(path) + File.separatorChar
                + CoordinationApplicationContext.SHARDING_SERVER_CONFIGURATION);
        ShardingServerConfig shardingServerConfig =
            JaxbUtil.unmarshal((String) configFile.getObj(), ShardingServerConfig.class);
        shardingServerConfigCache.put(path, shardingServerConfig);
      } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
        LOGGER.info("Please upload the sharding configuration file on zookeeper");
        throw new RuntimeException(e);
      } catch (JAXBException e1) {
        LOGGER.info("Unable to unmarshal the sharding xml configuration.");
        throw new RuntimeException(e1);
      }
    }
    return shardingServerConfigCache.get(path);
  }
  
  public static void putShardingClientConfig(String path, ShardingClientConfig shardingClientConfig){
    shardingClientConfigCache.put(path, shardingClientConfig);
  }
  
  public static void putShardingServerConfig(String path, ShardingServerConfig shardingServerConfig){
    shardingServerConfigCache.put(path, shardingServerConfig);
  }

  public static final FieldTypeArrayDataDescription getConfiguredDataDescription(String path) {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig(path);
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

  public static String getMaximumShardFileSizeInBytes(String path) {
    ShardingServerConfig shardingServerConfig = getShardingServerConfig(path);
    return shardingServerConfig.getMaximumShardFileSizeInBytes();

  }

  public static String[] getShardingDimensions(String path) {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig(path);
    String keyOrderString = shardingClientConfig.getShardingDimensions();
    return keyOrderString.split(",");
  }

  public static String[] getColumnsConfiguration(String path) {
    ShardingClientConfig shardingClientConfig = getShardingClientConfig(path);
    List<Column> columns = shardingClientConfig.getInput().getDataDescription().getColumn();
    String[] keyColumnNames = new String[columns.size()];
    for (int index = 0; index < columns.size(); index++) {
      keyColumnNames[index] = columns.get(index).getName();
    }
    return keyColumnNames;
  }

  public static String getKeysPrefix(String path) {
    ShardingServerConfig shardingServerConfig = getShardingServerConfig(path);
    return shardingServerConfig.getKeyPrefix();
  }

  public static int[] getShardingIndexes(String path) {
    String[] shardingKeys = getShardingDimensions(path);
    int[] shardingKeyIndexes = new int[shardingKeys.length];
    String keysNamingPrefix = getKeysPrefix(path);
    int keysNamingPrefixLength = keysNamingPrefix.length();
    for (int i = 0; i < shardingKeys.length; i++) {
      shardingKeyIndexes[i] =
          Integer.parseInt(shardingKeys[i].substring(keysNamingPrefixLength)) - 1;
    }
    return shardingKeyIndexes;
  }

  public static int getShardingIndexSequence(int keyId, String path) {
    int[] shardingIndexes = getShardingIndexes(path);
    int index = -1;
    for (int i = 0; i < shardingIndexes.length; i++) {
      if (shardingIndexes[i] == keyId) {
        index = i;
        break;
      }
    }
    return index;
  }

  public static void uploadShardingConfigOnZk(String shardingConfigPath,
      String nodeName, String configurationFile) throws IOException, InterruptedException,JAXBException {
    LOGGER.info("Updating sharding configuration on zookeeper");
    NodesManager manager = NodesManagerContext.getNodesManagerInstance();
    ZKNodeFile configFile = new ZKNodeFile(nodeName, configurationFile);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    manager.saveShardingConfigFileToZNode(shardingConfigPath, configFile, countDownLatch);
    countDownLatch.await();
  }

  public static String getShardingConfigFilePathOnZk(String distributedFilePath) {
    String fileSystemBasePath = CoordinationApplicationContext.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath();
    return fileSystemBasePath + distributedFilePath;
  }

}
