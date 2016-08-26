package com.talentica.hungryHippos.node;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class NodeUtil {

  private static final Logger logger = LoggerFactory.getLogger(NodeUtil.class);

  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;

  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = null;

  public NodeUtil(String filePath) {
    String shardingTempFolder = FileSystemContext.getRootDirectory() + filePath
        + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
    ShardingApplicationContext context = new ShardingApplicationContext(shardingTempFolder);
    Map<String,String> dataTypeMap = ShardingFileUtil.getDataTypeMap(context);
    String keyToValueToBucketMapFile = shardingTempFolder + File.separatorChar
        + ShardingApplicationContext.keyToValueToBucketMapFile;

    keyToValueToBucketMap =
        ShardingFileUtil.readFromFileKeyToValueToBucket(keyToValueToBucketMapFile,dataTypeMap);

    String bucketToNodeNumberMapFile = shardingTempFolder + File.separatorChar
        + ShardingApplicationContext.bucketToNodeNumberMapFile;
    bucketToNodeNumberMap =
        ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFile);
  }

  public final Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
    return keyToValueToBucketMap;
  }

  public final Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    return bucketToNodeNumberMap;
  }


  public void createTrieBucketToNodeNumberMap(
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

  public void createTrieKeyToValueToBucketMap(
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

  public void createTrieBucketCombinationToNodeNumbersMap(
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
