/**
 * 
 */
package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.utility.ZkNodeName;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author pooshans
 * @author sohanc
 *
 */
public class ShardingTableZkService {
  private final Logger LOGGER = LoggerFactory.getLogger(ShardingTableZkService.class.getName());
  private Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap = new HashMap<>();
  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();
  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<>();

  public ShardingTableZkService() {
    
  }

  /**
   * To upload the BucketCombinationNodeNumber to ZK. .
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  public void zkUploadBucketCombinationToNodeNumbersMap(String path)
      throws IOException, InterruptedException, IllegalArgumentException, IllegalAccessException {
    ZkUtils.saveObjectZkNode(path + File.separatorChar + ZkNodeName.SHARDING_TABLE.getName()
        + File.separatorChar + ZkNodeName.BUCKET_COMBINATION.getName(),
        getBucketCombinationToNodeNumbersMap());
  }

  /**
   * To upload the BucketNodeNumber to ZK.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  public void zkUploadBucketToNodeNumberMap(String path)
      throws IOException, InterruptedException, IllegalArgumentException, IllegalAccessException {
    ZkUtils.saveObjectZkNode(path + File.separatorChar + ZkNodeName.SHARDING_TABLE.getName()
        + File.separatorChar + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName(),
        getBucketToNodeNumberMap());
  }

  /**
   * To upload the KeyValueBucket to ZK.
   * 
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  public void zkUploadKeyToValueToBucketMap(String path)
      throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    ZkUtils.saveObjectZkNode(path + File.separatorChar + ZkNodeName.SHARDING_TABLE.getName()
        + File.separatorChar + ZkNodeName.KEY_TO_VALUE_TO_BUCKET.getName(),
        getKeyToValueToBucketMap());
  }

  /**
   * @return Map<String, Map<Object, Bucket<KeyValueFrequency>>>
   */
  @SuppressWarnings("unchecked")
  public Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
    try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
        new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
            + PathUtil.SEPARATOR_CHAR + ShardingApplicationContext.keyToValueToBucketMapFile))) {
      return (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) inKeyValueNodeNumberMap
          .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read keyValueNodeNumberMap. Please put the file in current directory");
    }
    return null;
  }

  /**
   * @return Map<String, Map<Bucket<KeyValueFrequency>, Node>>
   */
  @SuppressWarnings("unchecked")
  public Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    try (ObjectInputStream bucketToNodeNumberMapInputStream = new ObjectInputStream(
        new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
            + PathUtil.SEPARATOR_CHAR + ShardingApplicationContext.bucketToNodeNumberMapFile))) {
      return (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) bucketToNodeNumberMapInputStream
          .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read bucketToNodeNumberMap. Please put the file in current directory");
    }
    return null;
  }

  /**
   * @return Map<BucketCombination, Set<Node>>
   */
  @SuppressWarnings("unchecked")
  public Map<BucketCombination, Set<Node>> getBucketCombinationToNodeNumbersMap() {
    try (ObjectInputStream bucketCombinationToNodeNumbersMapStream = new ObjectInputStream(
        new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
            + PathUtil.SEPARATOR_CHAR + ShardingApplicationContext.bucketCombinationToNodeNumbersMapFile))) {
      return (Map<BucketCombination, Set<Node>>) bucketCombinationToNodeNumbersMapStream
          .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info(
          "Unable to read bucketCombinationToNodeNumbersMap. Please put the file in current directory");
    }
    return null;
  }

  /**
   * @return Map<BucketCombination, Set<Node>>
   */
  @SuppressWarnings("unchecked")
  public Map<BucketCombination, Set<Node>> readBucketCombinationToNodeNumbersMap(
      String shardingTablePath) {
    String bucketCombinationPath = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + shardingTablePath + File.separatorChar
        + ZkNodeName.SHARDING_TABLE.getName() + File.separatorChar
        + ZkNodeName.BUCKET_COMBINATION.getName();
    bucketCombinationToNodeNumbersMap =
        (Map<BucketCombination, Set<Node>>) ZkUtils.readObjectZkNode(bucketCombinationPath);
    return bucketCombinationToNodeNumbersMap;
  }

  /**
   * @return Map<String, Map<Bucket<KeyValueFrequency>, Node>>
   */
  @SuppressWarnings("unchecked")
  public Map<String, Map<Bucket<KeyValueFrequency>, Node>> readBucketToNodeNumberMap(
      String shardingTablePath) {
    String keyToBucketNumberPath = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + shardingTablePath + File.separatorChar
        + ZkNodeName.SHARDING_TABLE.getName() + File.separatorChar
        + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName();
    bucketToNodeNumberMap = (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) ZkUtils
        .readObjectZkNode(keyToBucketNumberPath);
    return bucketToNodeNumberMap;
  }

  /**
   * @return Map<String, Map<Object, Bucket<KeyValueFrequency>>>
   */
  @SuppressWarnings("unchecked")
  public Map<String, Map<Object, Bucket<KeyValueFrequency>>> readKeyToValueToBucketMap(
      String shardingTablePath) {
    String KeyToValueToBucketPath = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + shardingTablePath + File.separatorChar
        + ZkNodeName.SHARDING_TABLE.getName() + File.separatorChar
        + ZkNodeName.KEY_TO_VALUE_TO_BUCKET.getName();
    keyToValueToBucketMap = (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) ZkUtils
        .readObjectZkNode(KeyToValueToBucketPath);
    return keyToValueToBucketMap;
  }
}
