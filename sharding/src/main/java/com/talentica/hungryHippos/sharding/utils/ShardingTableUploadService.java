/**
 * 
 */
package com.talentica.hungryHippos.sharding.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;
import com.talentica.hungryHippos.coordination.utility.ZkNodeName;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author pooshans
 *
 */
public class ShardingTableUploadService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingTableUploadService.class
      .getName());

  private static NodesManager nodesManager = CoordinationApplicationContext
      .getNodesManagerIntances();
  private static Property<ZkProperty> zkproperty = CoordinationApplicationContext.getZkProperty();

  private static String baseConfigPath;
  private static String zkKeyToBucketPath;
  private static String zkNodes;
  private static String baseConfigBucketToNodePath;

  public static void zkUploadBucketCombinationToNodeNumbersMap() throws IOException,
      InterruptedException, IllegalArgumentException, IllegalAccessException {
    int bucketCombinationId = 0;
    for (Entry<BucketCombination, Set<Node>> entry : getBucketCombinationToNodeNumbersMap()
        .entrySet()) {
      createBasePathForBucketCombinationToNodeNumberMap(bucketCombinationId);
      zkUploadKeyTOBucket(entry);
      zkUploadNodes(entry);
      bucketCombinationId++;
    }
  }

  public static void zkUploadBucketToNodeNumberMap() throws IOException, InterruptedException,
      IllegalArgumentException, IllegalAccessException {
    buildBasePathBucketToNode();
    for (Entry<String, Map<Bucket<KeyValueFrequency>, Node>> entry : getBucketToNodeNumberMap()
        .entrySet()) {
      String key = entry.getKey();
      String keyToBucketPath = baseConfigBucketToNodePath + File.separatorChar + key;
      CountDownLatch counter = new CountDownLatch(1);
      nodesManager.createPersistentNode(keyToBucketPath, counter);
      counter.await();
      Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = entry.getValue();
      int keyToBucketPathId = 0;
      for (Entry<Bucket<KeyValueFrequency>, Node> bucketNodeEntry : bucketToNodeMap.entrySet()) {
        String keyToBucketIdPath = createKeyToBucketId(keyToBucketPath, keyToBucketPathId);
        Bucket<KeyValueFrequency> bucket = bucketNodeEntry.getKey();
        Node node = bucketNodeEntry.getValue();
        createBucket(keyToBucketIdPath, bucket);
        createNode(keyToBucketIdPath, node);
        keyToBucketPathId++;
      }
    }
  }

  private static String createKeyToBucketId(String keyToBucketPath, int keyToBucketPathId) {
    keyToBucketPath =
        keyToBucketPath
            + File.separatorChar
            + (ZkNodeName.BUCKET.getName() + ZkNodeName.UNDERSCORE.getName()
                + ZkNodeName.NODE.getName() + ZkNodeName.UNDERSCORE.getName()
                + ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + keyToBucketPathId);
    return keyToBucketPath;
  }

  private static void createNode(String keyToBucketPath, Node node) throws IllegalAccessException,
      IOException, InterruptedException {
    CountDownLatch counter;
    String nodepath =
        keyToBucketPath + File.separatorChar
            + (ZkNodeName.NODE.getName() + ZkNodeName.UNDERSCORE.getName() + node.getNodeId());

    Field[] nodeFields = node.getClass().getDeclaredFields();
    counter = new CountDownLatch(nodeFields.length - 1);
    for (int fieldIndex = 0; fieldIndex < nodeFields.length; fieldIndex++) {
      nodeFields[fieldIndex].setAccessible(true);
      String fieldName = nodeFields[fieldIndex].getName();
      if (fieldName.equals("serialVersionUID")) {
        continue;
      }
      Object value = nodeFields[fieldIndex].get(node);
      String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
      String leafNodePath = nodepath + File.separatorChar + leafNode;
      nodesManager.createPersistentNode(leafNodePath, counter);
    }
    counter.await();
  }

  private static void createBucket(String keyToBucketPath, Bucket<KeyValueFrequency> bucket)
      throws IllegalAccessException, IOException, InterruptedException {
    CountDownLatch counter;
    String bucketPath =
        keyToBucketPath + File.separatorChar
            + (ZkNodeName.BUCKET.getName() + ZkNodeName.UNDERSCORE.getName() + bucket.getId());

    Field[] bucketfields = bucket.getClass().getDeclaredFields();
    counter = new CountDownLatch(bucketfields.length - 1);
    for (int fieldIndex = 0; fieldIndex < bucketfields.length; fieldIndex++) {
      bucketfields[fieldIndex].setAccessible(true);
      String fieldName = bucketfields[fieldIndex].getName();
      if (fieldName.equals("serialVersionUID")) {
        continue;
      }
      Object value = bucketfields[fieldIndex].get(bucket);
      String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
      String leafBucketPath = bucketPath + File.separatorChar + leafNode;
      nodesManager.createPersistentNode(leafBucketPath, counter);
    }
    counter.await();
  }

  private static void buildBasePathBucketToNode() {
    baseConfigBucketToNodePath =
        zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
            + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName();
  }

  private static void createBasePathForBucketCombinationToNodeNumberMap(int bucketCombinationId) {
    baseConfigPath =
        zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
            + ZkNodeName.BUCKET_COMBINATION.getName() + File.separatorChar;

    zkKeyToBucketPath =
        baseConfigPath
            + (ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + bucketCombinationId)
            + File.separatorChar + ZkNodeName.KEY_TO_BUCKET.getName();
    zkNodes =
        baseConfigPath
            + (ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + bucketCombinationId)
            + File.separatorChar + ZkNodeName.NODES.getName();
  }

  private static void zkUploadNodes(Entry<BucketCombination, Set<Node>> entry)
      throws IllegalAccessException, IOException, InterruptedException {
    Set<Node> nodes = entry.getValue();
    int nodeId = 0;
    for (Node node : nodes) {
      Field[] fields = node.getClass().getDeclaredFields();
      CountDownLatch counter = new CountDownLatch(fields.length - 1);
      for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
        fields[fieldIndex].setAccessible(true);
        String fieldName = fields[fieldIndex].getName();
        if (fieldName.equals("serialVersionUID")) {
          continue;
        }
        Object value = fields[fieldIndex].get(node);
        String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
        String leafNodePath =
            zkNodes + File.separatorChar
                + (ZkNodeName.NODE.getName() + ZkNodeName.UNDERSCORE.getName() + nodeId)
                + File.separatorChar + leafNode;
        nodesManager.createPersistentNode(leafNodePath, counter);
      }
      nodeId++;
      counter.await();
    }
  }

  private static void zkUploadKeyTOBucket(Entry<BucketCombination, Set<Node>> entry)
      throws IllegalAccessException, IOException, InterruptedException {
    Map<String, Bucket<KeyValueFrequency>> bucketCombinationMap =
        entry.getKey().getBucketsCombination();
    for (Entry<String, Bucket<KeyValueFrequency>> bucketCombination : bucketCombinationMap
        .entrySet()) {
      Bucket<KeyValueFrequency> bucket = bucketCombination.getValue();
      String key = bucketCombination.getKey();
      Field[] fields = bucket.getClass().getDeclaredFields();
      CountDownLatch counter = new CountDownLatch(fields.length - 1);
      for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
        fields[fieldIndex].setAccessible(true);
        String fieldName = fields[fieldIndex].getName();
        if (fieldName.equals("serialVersionUID")) {
          continue;
        }
        Object value = fields[fieldIndex].get(bucket);
        String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
        String leafNodePath =
            zkKeyToBucketPath + File.separatorChar + key + File.separatorChar
                + ZkNodeName.BUCKET.getName() + File.separatorChar + leafNode;
        nodesManager.createPersistentNode(leafNodePath, counter);
      }
      counter.await();
    }
  }

  private static Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
    try (ObjectInputStream inKeyValueNodeNumberMap =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.keyToValueToBucketMapFile))) {
      return (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) inKeyValueNodeNumberMap
          .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read keyValueNodeNumberMap. Please put the file in current directory");
    }
    return null;
  }

  private static Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    try (ObjectInputStream bucketToNodeNumberMapInputStream =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.bucketToNodeNumberMapFile))) {
      return (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) bucketToNodeNumberMapInputStream
          .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read bucketToNodeNumberMap. Please put the file in current directory");
    }
    return null;
  }

  private static Map<BucketCombination, Set<Node>> getBucketCombinationToNodeNumbersMap() {
    try (ObjectInputStream bucketToNodeNumberMapInputStream =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.bucketCombinationToNodeNumbersMapFile))) {
      return (Map<BucketCombination, Set<Node>>) bucketToNodeNumberMapInputStream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER
          .info("Unable to read bucketCombinationToNodeNumbersMap. Please put the file in current directory");
    }
    return null;
  }
}
