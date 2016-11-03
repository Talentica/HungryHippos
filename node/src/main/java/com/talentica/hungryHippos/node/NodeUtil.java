package com.talentica.hungryHippos.node;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * {@code NodeUtil} contains the utility methods for creating Tries on the basis of the map.
 * 
 */
public class NodeUtil {

  private static final Logger logger = LoggerFactory.getLogger(NodeUtil.class);

  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;

  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = null;

  /**
   * creates an instance of NodeUtil.
   * 
   * @param filePath
   */
  public NodeUtil(String filePath) {
    String shardingTempFolder = FileSystemContext.getRootDirectory() + filePath + File.separatorChar
        + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
    ShardingApplicationContext context = new ShardingApplicationContext(shardingTempFolder);
    Map<String, String> dataTypeMap = ShardingFileUtil.getDataTypeMap(context);
    String keyToValueToBucketMapFile = shardingTempFolder + File.separatorChar
        + ShardingApplicationContext.keyToValueToBucketMapFile;

    keyToValueToBucketMap =
        ShardingFileUtil.readFromFileKeyToValueToBucket(keyToValueToBucketMapFile, dataTypeMap);

    String bucketToNodeNumberMapFile = shardingTempFolder + File.separatorChar
        + ShardingApplicationContext.bucketToNodeNumberMapFile;
    bucketToNodeNumberMap =
        ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFile);
  }

  /**
   * retrieves the key to value map associated with this object.
   * 
   * @return
   */
  public final Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
    return keyToValueToBucketMap;
  }

  /**
   * retrieves the bucket to node map associated with this object.
   * 
   * @return
   */
  public final Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    return bucketToNodeNumberMap;
  }


  /**
   * creates Trie based on the map provided.
   * 
   * @param bucketToNodeNumberMap
   * @param curator
   * @throws IOException
   * @throws HungryHippoException
   */
  public void createTrieBucketToNodeNumberMap(
      Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      HungryHippoCurator curator) throws IOException, HungryHippoException {
    String buildPath = curator.buildConfigPath("bucketToNodeNumberMap");

    for (String keyName : bucketToNodeNumberMap.keySet()) {
      String keyNodePath = buildPath + PathUtil.SEPARATOR_CHAR + keyName;
      curator.createPersistentNodeIfNotPresent(keyNodePath);
      logger.info("Path {} is created", keyNodePath);
      Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = bucketToNodeNumberMap.get(keyName);
      for (Bucket<KeyValueFrequency> bucketKey : bucketToNodeMap.keySet()) {
        String buildBucketNodePath = keyNodePath;
        curator.createPersistentNodeIfNotPresent(buildBucketNodePath);
        String buildBucketKeyPath =
            buildBucketNodePath + PathUtil.SEPARATOR_CHAR + bucketKey.toString();
        curator.createPersistentNode(buildBucketKeyPath, null);
        String buildNodePath = buildBucketKeyPath + PathUtil.SEPARATOR_CHAR
            + bucketToNodeMap.get(bucketKey).toString();
        curator.createPersistentNodeIfNotPresent(buildNodePath);

        logger.info("Path {} is created", buildBucketNodePath);
      }
    }
  }

  /**
   * creates a Trie.
   * 
   * @param keyToValueToBucketMap
   * @param curator
   * @throws IOException
   * @throws HungryHippoException
   */
  public void createTrieKeyToValueToBucketMap(
      Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap,
      HungryHippoCurator curator) throws IOException, HungryHippoException {
    String buildPath = curator.buildConfigPath("keyToValueToBucketMap");
    for (String keyName : keyToValueToBucketMap.keySet()) {
      String keyNodePath = buildPath + PathUtil.SEPARATOR_CHAR + keyName;
      curator.createPersistentNodeIfNotPresent(keyNodePath);
      logger.info("Path {} is created", keyNodePath);
      Map<Object, Bucket<KeyValueFrequency>> valueBucketMap = keyToValueToBucketMap.get(keyName);
      for (Object valueKey : valueBucketMap.keySet()) {
        String buildValueBucketPath = keyNodePath;
        curator.createPersistentNodeIfNotPresent(buildValueBucketPath);
        String buildValueKeyPath = buildValueBucketPath + PathUtil.SEPARATOR_CHAR + valueKey;
        curator.createPersistentNodeIfNotPresent(buildValueKeyPath);
        String buildBucketPath =
            buildValueKeyPath + PathUtil.SEPARATOR_CHAR + valueBucketMap.get(valueKey).toString();
        curator.createPersistentNodeIfNotPresent(buildBucketPath);

        logger.info("Path {} is created", buildValueBucketPath);
      }
    }
  }

  /**
   * creates Trie.
   * 
   * @param bucketCombinationToNodeNumbersMap
   * @param curator
   * @throws IOException
   * @throws HungryHippoException
   */
  public void createTrieBucketCombinationToNodeNumbersMap(
      Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap,
      HungryHippoCurator curator) throws IOException, HungryHippoException {
    String buildPath = curator.buildConfigPath("bucketCombinationToNodeNumbersMap");
    for (BucketCombination bucketCombinationKey : bucketCombinationToNodeNumbersMap.keySet()) {
      String keyNodePath = buildPath + PathUtil.SEPARATOR_CHAR + bucketCombinationKey.toString();
      Set<Node> nodes = bucketCombinationToNodeNumbersMap.get(bucketCombinationKey);
      for (Node node : nodes) {
        String buildBucketCombinationNodeNumberPath =
            keyNodePath + PathUtil.SEPARATOR_CHAR + node.toString();
        curator.createPersistentNodeIfNotPresent(buildBucketCombinationNodeNumberPath);
        logger.info("Path {} is created", buildBucketCombinationNodeNumberPath);
      }
    }
  }
}
