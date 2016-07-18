package com.talentica.hungryHippos.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.ShardingTableCache;
import com.talentica.hungryHippos.sharding.ShardingTableFilesName;
import com.talentica.hungryHippos.utility.PathUtil;

@SuppressWarnings("unchecked")
public class NodeUtil {

  private static final String nodeIdFile = "nodeId";

  private static final Logger logger = LoggerFactory.getLogger(NodeUtil.class);

  private static Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;

  private static Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = null;

  static {
    ShardingTableCache shardingTableCache = ShardingTableCache.newInstance();
    keyToValueToBucketMap = (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) shardingTableCache
        .getShardingTableFromCache(
            ShardingTableFilesName.KEY_TO_VALUE_TO_BUCKET_MAP_FILE.getName());
    bucketToNodeNumberMap = (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) shardingTableCache
        .getShardingTableFromCache(ShardingTableFilesName.BUCKET_TO_NODE_NUMBER_MAP_FILE.getName());
  }

  public static final Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
    return keyToValueToBucketMap;
  }

  public static final Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    return bucketToNodeNumberMap;
  }

  /**
   * Read the file nodeId which contains nodeId value.
   * 
   * @return NodeId
   * @throws Exception
   */
  public static int getNodeId() throws IOException {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(
          new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
              + PathUtil.SEPARATOR_CHAR + nodeIdFile)));
      String line = in.readLine();
      in.close();
      return Integer.parseInt(line);
    } catch (IOException exception) {
      logger.info("Unable to read the file for NODE ID. Exiting..");
      throw exception;
    }
  }

  public static void createTrieBucketToNodeNumberMap(
      Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      NodesManager nodesManager) throws IOException {
    String buildPath = nodesManager.buildConfigPath("bucketToNodeNumberMap");

    for (String keyName : bucketToNodeNumberMap.keySet()) {
      String keyNodePath = buildPath + PathUtil.SEPARATOR_CHAR + keyName;
      nodesManager.createPersistentNode(keyNodePath, null);
      logger.info("Path {} is created", keyNodePath);
      Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = bucketToNodeNumberMap.get(keyName);
      for (Bucket<KeyValueFrequency> bucketKey : bucketToNodeMap.keySet()) {
        String buildBucketNodePath = keyNodePath;
        nodesManager.createPersistentNode(buildBucketNodePath, null);
        String buildBucketKeyPath =
            buildBucketNodePath + PathUtil.SEPARATOR_CHAR + bucketKey.toString();
        nodesManager.createPersistentNode(buildBucketKeyPath, null);
        String buildNodePath = buildBucketKeyPath + PathUtil.SEPARATOR_CHAR
            + bucketToNodeMap.get(bucketKey).toString();
        nodesManager.createPersistentNode(buildNodePath, null);
        // nodesManager.createPersistentNode(buildBucketNodePath, null);
        logger.info("Path {} is created", buildBucketNodePath);
      }
    }
  }

  public static void createTrieKeyToValueToBucketMap(
      Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap,
      NodesManager nodesManager) throws IOException {
    String buildPath = nodesManager.buildConfigPath("keyToValueToBucketMap");
    for (String keyName : keyToValueToBucketMap.keySet()) {
      String keyNodePath = buildPath + PathUtil.SEPARATOR_CHAR + keyName;
      nodesManager.createPersistentNode(keyNodePath, null);
      logger.info("Path {} is created", keyNodePath);
      Map<Object, Bucket<KeyValueFrequency>> valueBucketMap = keyToValueToBucketMap.get(keyName);
      for (Object valueKey : valueBucketMap.keySet()) {
        String buildValueBucketPath = keyNodePath;
        nodesManager.createPersistentNode(buildValueBucketPath, null);
        String buildValueKeyPath = buildValueBucketPath + PathUtil.SEPARATOR_CHAR + valueKey;
        nodesManager.createPersistentNode(buildValueKeyPath, null);
        String buildBucketPath =
            buildValueKeyPath + PathUtil.SEPARATOR_CHAR + valueBucketMap.get(valueKey).toString();
        nodesManager.createPersistentNode(buildBucketPath, null);
        // nodesManager.createPersistentNode(buildValueBucketPath,
        // null);

        logger.info("Path {} is created", buildValueBucketPath);
      }
    }
  }

  public static void createTrieBucketCombinationToNodeNumbersMap(
      Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap,
      NodesManager nodesManager) throws IOException {
    String buildPath = nodesManager.buildConfigPath("bucketCombinationToNodeNumbersMap");
    for (BucketCombination bucketCombinationKey : bucketCombinationToNodeNumbersMap.keySet()) {
      String keyNodePath = buildPath + PathUtil.SEPARATOR_CHAR + bucketCombinationKey.toString();
      Set<Node> nodes = bucketCombinationToNodeNumbersMap.get(bucketCombinationKey);
      for (Node node : nodes) {
        String buildBucketCombinationNodeNumberPath =
            keyNodePath + PathUtil.SEPARATOR_CHAR + node.toString();
        nodesManager.createPersistentNode(buildBucketCombinationNodeNumberPath, null);
        logger.info("Path {} is created", buildBucketCombinationNodeNumberPath);
      }
    }
  }
}
