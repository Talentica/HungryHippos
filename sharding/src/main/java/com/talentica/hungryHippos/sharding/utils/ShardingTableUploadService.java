/**
 * 
 */
package com.talentica.hungryHippos.sharding.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.property.ZkProperty;
import com.talentica.hungryHippos.coordination.utility.ZkNodeName;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryhippos.config.client.CoordinationServers;

/**
 * @author pooshans
 *
 */
public class ShardingTableUploadService {

  private final Logger LOGGER = LoggerFactory.getLogger(ShardingTableUploadService.class.getName());

  private static NodesManager nodesManager;
  private static Property<ZkProperty> zkproperty = CoordinationApplicationContext.getZkProperty();
  private String baseConfigPath;
  private String zkKeyToBucketPath;
  private String zkNodes;
  private String baseConfigBucketToNodePath;
  private String baseConfigKeyToValueToBucketPath;

  public ShardingTableUploadService() throws FileNotFoundException, JAXBException {
    nodesManager = NodesManagerContext.getNodesManagerInstance();
  }

  public void zkUploadBucketCombinationToNodeNumbersMap() throws IOException, InterruptedException,
      IllegalArgumentException, IllegalAccessException {
    int bucketCombinationId = 0;
    for (Entry<BucketCombination, Set<Node>> entry : getBucketCombinationToNodeNumbersMap()
        .entrySet()) {
      createBasePathBucketCombinationToNodeNumberMap(bucketCombinationId);
      zkUploadKeyToBucket(entry);
      zkUploadNodes(entry);
      bucketCombinationId++;
    }
  }

  public void zkUploadBucketToNodeNumberMap() throws IOException, InterruptedException,
      IllegalArgumentException, IllegalAccessException {
    buildBasePathBucketToNode();
    for (Entry<String, Map<Bucket<KeyValueFrequency>, Node>> entry : getBucketToNodeNumberMap()
        .entrySet()) {
      String key = entry.getKey();
      String keyToBucketPath = baseConfigBucketToNodePath + File.separatorChar + key;
      createZkBasePath(keyToBucketPath);
      Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = entry.getValue();
      int keyToBucketPathId = 0;
      createZkBucketNode(keyToBucketPath, bucketToNodeMap, keyToBucketPathId);
    }
  }

  public void zkUploadKeyToValueToBucketMap() throws IllegalArgumentException,
      IllegalAccessException, IOException, InterruptedException {
    for (Entry<String, Map<Object, Bucket<KeyValueFrequency>>> entry : getKeyToValueToBucketMap()
        .entrySet()) {
      buildBaseConfigKeyToValueToBucketPath();
      createZkKeyToValueBucket(entry);
    }
  }

  private void createZkKeyToValueBucket(Entry<String, Map<Object, Bucket<KeyValueFrequency>>> entry)
      throws IllegalAccessException, IOException, InterruptedException {
    String keyPath = baseConfigKeyToValueToBucketPath + File.separatorChar + entry.getKey();
    Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = entry.getValue();
    for (Entry<Object, Bucket<KeyValueFrequency>> valueToBucketEntry : valueToBucketMap.entrySet()) {
      Object value = valueToBucketEntry.getKey();
      String valuePath = keyPath + File.separatorChar + value;
      Bucket<KeyValueFrequency> bucket = valueToBucketEntry.getValue();
      createZkNode(valuePath, bucket);
    }
  }

  private void createZkNode(String valuePath, Bucket<KeyValueFrequency> bucket)
      throws IllegalAccessException, IOException, InterruptedException {
    Field[] bucketFields = bucket.getClass().getDeclaredFields();
    CountDownLatch counter = new CountDownLatch(bucketFields.length - 1);
    for (int fieldIndex = 0; fieldIndex < bucketFields.length; fieldIndex++) {
      bucketFields[fieldIndex].setAccessible(true);
      String fieldName = bucketFields[fieldIndex].getName();
      if (fieldName.equals("serialVersionUID")) {
        continue;
      }
      Object bucketValue = bucketFields[fieldIndex].get(bucket);
      String leafNode = fieldName + ZkNodeName.EQUAL.getName() + bucketValue;
      String leafNodePath =
          valuePath + File.separatorChar + ZkNodeName.BUCKET.getName() + File.separatorChar
              + leafNode;
      nodesManager.createPersistentNode(leafNodePath, counter);
    }
    counter.await();
  }

  private void createZkBucketNode(String keyToBucketPath,
      Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap, int keyToBucketPathId)
      throws IllegalAccessException, IOException, InterruptedException {
    for (Entry<Bucket<KeyValueFrequency>, Node> bucketNodeEntry : bucketToNodeMap.entrySet()) {
      String keyToBucketIdPath = createZkKeyToBucketId(keyToBucketPath, keyToBucketPathId);
      Bucket<KeyValueFrequency> bucket = bucketNodeEntry.getKey();
      Node node = bucketNodeEntry.getValue();
      createZkBucket(keyToBucketIdPath, bucket);
      createZkNode(keyToBucketIdPath, node);
      keyToBucketPathId++;
    }
  }

  private void createZkBasePath(String keyToBucketPath) throws IOException, InterruptedException {
    CountDownLatch counter = new CountDownLatch(1);
    nodesManager.createPersistentNode(keyToBucketPath, counter);
    counter.await();
  }

  private String createZkKeyToBucketId(String keyToBucketPath, int keyToBucketPathId) {
    keyToBucketPath =
        keyToBucketPath
            + File.separatorChar
            + (ZkNodeName.BUCKET.getName() + ZkNodeName.UNDERSCORE.getName()
                + ZkNodeName.NODE.getName() + ZkNodeName.UNDERSCORE.getName()
                + ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + keyToBucketPathId);
    return keyToBucketPath;
  }

  private void createZkNode(String keyToBucketPath, Node node) throws IllegalAccessException,
      IOException, InterruptedException {
    CountDownLatch counter;
    String nodepath = keyToBucketPath + File.separatorChar + (ZkNodeName.NODE.getName());

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

  private void createZkBucket(String keyToBucketPath, Bucket<KeyValueFrequency> bucket)
      throws IllegalAccessException, IOException, InterruptedException {
    CountDownLatch counter;
    String bucketPath = keyToBucketPath + File.separatorChar + (ZkNodeName.BUCKET.getName());

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

  /*
   * private void buildBasePathBucketToNode() { baseConfigBucketToNodePath =
   * zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar +
   * ZkNodeName.KEY_TO_BUCKET_NUMBER.getName(); }
   * 
   * private void createBasePathForBucketCombinationToNodeNumberMap(int bucketCombinationId) {
   * baseConfigPath = zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar +
   * ZkNodeName.BUCKET_COMBINATION.getName() + File.separatorChar;
   * 
   * zkKeyToBucketPath = baseConfigPath + (ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName()
   * + bucketCombinationId) + File.separatorChar + ZkNodeName.KEY_TO_BUCKET.getName(); zkNodes =
   * baseConfigPath + (ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() +
   * bucketCombinationId) + File.separatorChar + ZkNodeName.NODES.getName(); }
   */

  private void zkUploadNodes(Entry<BucketCombination, Set<Node>> entry)
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

  private void zkUploadKeyToBucket(Entry<BucketCombination, Set<Node>> entry)
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

  private void buildBaseConfigKeyToValueToBucketPath() {
    baseConfigKeyToValueToBucketPath =
        zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
            + ZkNodeName.KEY_TO_VALUE_TO_BUCKET.getName();
  }

  private void buildBasePathBucketToNode() {
    baseConfigBucketToNodePath =
        zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
            + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName();
  }

  private void createBasePathBucketCombinationToNodeNumberMap(int bucketCombinationId) {
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


  @SuppressWarnings("unchecked")
  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
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

  @SuppressWarnings("unchecked")
  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
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

  @SuppressWarnings("unchecked")
  private Map<BucketCombination, Set<Node>> getBucketCombinationToNodeNumbersMap() {
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
