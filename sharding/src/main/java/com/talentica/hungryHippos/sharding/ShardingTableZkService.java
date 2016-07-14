/**
 * 
 */
package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.annotations.ZkTransient;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.ZkNodeName;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author pooshans
 * @author sohanc
 *
 */
public class ShardingTableZkService {
  private static NodesManager nodesManager;
  private final Logger LOGGER = LoggerFactory.getLogger(ShardingTableZkService.class.getName());
  private String baseConfigPath;
  private String zkKeyToBucketPath;
  private String zkNodes;
  private String baseConfigBucketToNodePath;
  private String baseConfigKeyToValueToBucketPath;
  private Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap = new HashMap<>();
  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();
  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<>();
  private static String bucketCombinationPath;
  private static String keyToBucketNumberPath;
  private static String KeyToValueToBucketPath;

  public ShardingTableZkService() {
    try {
      nodesManager = NodesManagerContext.getNodesManagerInstance();
    } catch (FileNotFoundException | JAXBException e) {
      LOGGER.error("Unable to start the nodeManager");
    }
  }

  /**
   * To upload the BucketCombinationNodeNumber to ZK. .
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
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

  /**
   * To upload the BucketNodeNumber to ZK.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
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

  /**
   * To upload the KeyValueBucket to ZK.
   * 
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  public void zkUploadKeyToValueToBucketMap() throws IllegalArgumentException,
      IllegalAccessException, IOException, InterruptedException {
    for (Entry<String, Map<Object, Bucket<KeyValueFrequency>>> entry : getKeyToValueToBucketMap()
        .entrySet()) {
      buildBaseConfigKeyToValueToBucketPath();
      createZkKeyToValueBucket(entry);
    }
  }

  /**
   * To create the KeyToValueBucket on ZK.
   * 
   * @param entry
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
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

  /**
   * To create the Node on ZK.
   * 
   * @param valuePath
   * @param bucket
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  private void createZkNode(String valuePath, Bucket<KeyValueFrequency> bucket)
      throws IllegalAccessException, IOException, InterruptedException {
    Field[] bucketFields = bucket.getClass().getDeclaredFields();
    CountDownLatch counter = new CountDownLatch(bucketFields.length);
    for (int fieldIndex = 0; fieldIndex < bucketFields.length; fieldIndex++) {
      bucketFields[fieldIndex].setAccessible(true);
      String fieldName = bucketFields[fieldIndex].getName();
      boolean isTransient = ZKUtils.isZkTransient(bucketFields, fieldIndex);
      Object bucketValue = bucketFields[fieldIndex].get(bucket);
      String leafNode = fieldName + ZkNodeName.EQUAL.getName() + bucketValue;
      String leafNodePath =
          valuePath + File.separatorChar + ZkNodeName.BUCKET.getName() + File.separatorChar
              + leafNode;
      nodesManager.createPersistentNode(leafNodePath, counter, isTransient);
    }
    counter.await();
  }

  /**
   * To create the Bucket node on ZK.
   * 
   * @param keyToBucketPath
   * @param bucketToNodeMap
   * @param keyToBucketPathId
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
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

  /**
   * @param keyToBucketPath
   * @throws IOException
   * @throws InterruptedException
   */
  private void createZkBasePath(String keyToBucketPath) throws IOException, InterruptedException {
    CountDownLatch counter = new CountDownLatch(1);
    nodesManager.createPersistentNode(keyToBucketPath, counter);
    counter.await();
  }

  /**
   * @param keyToBucketPath
   * @param keyToBucketPathId
   * @return
   */
  private String createZkKeyToBucketId(String keyToBucketPath, int keyToBucketPathId) {
    keyToBucketPath =
        keyToBucketPath
            + File.separatorChar
            + (ZkNodeName.BUCKET.getName() + ZkNodeName.UNDERSCORE.getName()
                + ZkNodeName.NODE.getName() + ZkNodeName.UNDERSCORE.getName()
                + ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + keyToBucketPathId);
    return keyToBucketPath;
  }

  /**
   * @param keyToBucketPath
   * @param node
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  private void createZkNode(String keyToBucketPath, Node node) throws IllegalAccessException,
      IOException, InterruptedException {
    CountDownLatch counter;
    String nodepath = keyToBucketPath + File.separatorChar + (ZkNodeName.NODE.getName());

    Field[] nodeFields = node.getClass().getDeclaredFields();
    counter = new CountDownLatch(nodeFields.length);
    for (int fieldIndex = 0; fieldIndex < nodeFields.length; fieldIndex++) {
      nodeFields[fieldIndex].setAccessible(true);
      String fieldName = nodeFields[fieldIndex].getName();
      boolean isTransient = ZKUtils.isZkTransient(nodeFields, fieldIndex);
      Object value = nodeFields[fieldIndex].get(node);
      String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
      String leafNodePath = nodepath + File.separatorChar + leafNode;
      nodesManager.createPersistentNode(leafNodePath, counter, isTransient);
    }
    counter.await();
  }

  /**
   * @param keyToBucketPath
   * @param bucket
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  private void createZkBucket(String keyToBucketPath, Bucket<KeyValueFrequency> bucket)
      throws IllegalAccessException, IOException, InterruptedException {
    CountDownLatch counter;
    String bucketPath = keyToBucketPath + File.separatorChar + (ZkNodeName.BUCKET.getName());

    Field[] bucketfields = bucket.getClass().getDeclaredFields();
    counter = new CountDownLatch(bucketfields.length);
    for (int fieldIndex = 0; fieldIndex < bucketfields.length; fieldIndex++) {
      bucketfields[fieldIndex].setAccessible(true);
      String fieldName = bucketfields[fieldIndex].getName();
      boolean isTransient = ZKUtils.isZkTransient(bucketfields, fieldIndex);
      Object value = bucketfields[fieldIndex].get(bucket);
      String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
      String leafBucketPath = bucketPath + File.separatorChar + leafNode;
      nodesManager.createPersistentNode(leafBucketPath, counter, isTransient);
    }
    counter.await();
  }

  /**
   * @param entry
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  private void zkUploadNodes(Entry<BucketCombination, Set<Node>> entry)
      throws IllegalAccessException, IOException, InterruptedException {
    Set<Node> nodes = entry.getValue();
    int nodeId = 0;
    for (Node node : nodes) {
      Field[] fields = node.getClass().getDeclaredFields();
      CountDownLatch counter = new CountDownLatch(fields.length);
      for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
        fields[fieldIndex].setAccessible(true);
        String fieldName = fields[fieldIndex].getName();
        boolean isTransient = ZKUtils.isZkTransient(fields, fieldIndex);
        Object value = fields[fieldIndex].get(node);
        String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
        String leafNodePath =
            zkNodes + File.separatorChar
                + (ZkNodeName.NODE.getName() + ZkNodeName.UNDERSCORE.getName() + nodeId)
                + File.separatorChar + leafNode;
        nodesManager.createPersistentNode(leafNodePath, counter, isTransient);
      }
      nodeId++;
      counter.await();
    }
  }

  /**
   * @param entry
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  private void zkUploadKeyToBucket(Entry<BucketCombination, Set<Node>> entry)
      throws IllegalAccessException, IOException, InterruptedException {
    Map<String, Bucket<KeyValueFrequency>> bucketCombinationMap =
        entry.getKey().getBucketsCombination();
    for (Entry<String, Bucket<KeyValueFrequency>> bucketCombination : bucketCombinationMap
        .entrySet()) {
      Bucket<KeyValueFrequency> bucket = bucketCombination.getValue();
      String key = bucketCombination.getKey();
      Field[] fields = bucket.getClass().getDeclaredFields();
      CountDownLatch counter = new CountDownLatch(fields.length);
      for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
        fields[fieldIndex].setAccessible(true);
        String fieldName = fields[fieldIndex].getName();
        boolean isTransient = ZKUtils.isZkTransient(fields, fieldIndex);
        Object value = fields[fieldIndex].get(bucket);
        String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
        String leafNodePath =
            zkKeyToBucketPath + File.separatorChar + key + File.separatorChar
                + ZkNodeName.BUCKET.getName() + File.separatorChar + leafNode;
        nodesManager.createPersistentNode(leafNodePath, counter, isTransient);
      }
      counter.await();
    }
  }

  private void buildBaseConfigKeyToValueToBucketPath() {
    baseConfigKeyToValueToBucketPath =
        NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting()
            .getShardingTablePath()
            + File.separatorChar + ZkNodeName.KEY_TO_VALUE_TO_BUCKET.getName();
  }

  private void buildBasePathBucketToNode() {
    baseConfigBucketToNodePath =
        NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting()
            .getShardingTablePath()
            + File.separatorChar + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName();
  }

  private void createBasePathBucketCombinationToNodeNumberMap(int bucketCombinationId) {
    baseConfigPath =
        NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting()
            .getShardingTablePath()
            + File.separatorChar + ZkNodeName.BUCKET_COMBINATION.getName() + File.separatorChar;

    zkKeyToBucketPath =
        baseConfigPath
            + (ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + bucketCombinationId)
            + File.separatorChar + ZkNodeName.KEY_TO_BUCKET.getName();
    zkNodes =
        baseConfigPath
            + (ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + bucketCombinationId)
            + File.separatorChar + ZkNodeName.NODES.getName();
  }


  /**
   * @return Map<String, Map<Object, Bucket<KeyValueFrequency>>>
   */
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

  /**
   * @return Map<String, Map<Bucket<KeyValueFrequency>, Node>>
   */
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

  /**
   * @return Map<BucketCombination, Set<Node>>
   */
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

  /**
   * @return Map<BucketCombination, Set<Node>>
   */
  public Map<BucketCombination, Set<Node>> readBucketCombinationToNodeNumbersMap() {
    bucketCombinationPath =
        NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting()
            .getShardingTablePath()
            + File.separatorChar + ZkNodeName.BUCKET_COMBINATION.getName();
    try {
      List<String> children = nodesManager.getChildren(bucketCombinationPath);
      if (children != null) {
        for (String child : children) {
          String keyToBucketMapPath =
              bucketCombinationPath + File.separatorChar + child + File.separatorChar
                  + ZkNodeName.KEY_TO_BUCKET.getName();
          List<String> keyToBucketChildren = nodesManager.getChildren(keyToBucketMapPath);
          Map<String, Bucket<KeyValueFrequency>> keyToBucketMap =
              new HashMap<String, Bucket<KeyValueFrequency>>();
          for (String keyToBucketChild : keyToBucketChildren) {
            String bucketPath =
                keyToBucketMapPath + File.separatorChar + keyToBucketChild + File.separatorChar
                    + ZkNodeName.BUCKET.getName();
            List<String> bucketChildren = nodesManager.getChildren(bucketPath);
            Bucket<KeyValueFrequency> bucket = createBucket(bucketChildren);
            keyToBucketMap.put(keyToBucketChild, bucket);
          }
          BucketCombination bucketCombination = new BucketCombination(keyToBucketMap);
          String nodesPath =
              bucketCombinationPath + File.separatorChar + child + File.separatorChar
                  + ZkNodeName.NODES.getName();
          List<String> nodesChildren = nodesManager.getChildren(nodesPath);
          Set<Node> nodesForBucketCombination = new HashSet<>();
          for (String nodeChild : nodesChildren) {
            String nodePath = nodesPath + File.separatorChar + nodeChild;
            List<String> nodeChildren = nodesManager.getChildren(nodePath);
            Node node = createNode(nodeChildren);
            nodesForBucketCombination.add(node);
          }
          bucketCombinationToNodeNumbersMap.put(bucketCombination, nodesForBucketCombination);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.error(e.getMessage());
    }
    return bucketCombinationToNodeNumbersMap;
  }

  /**
   * @return Map<String, Map<Bucket<KeyValueFrequency>, Node>>
   */
  public Map<String, Map<Bucket<KeyValueFrequency>, Node>> readBucketToNodeNumberMap() {
    keyToBucketNumberPath =
        NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting()
            .getShardingTablePath()
            + File.separatorChar + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName();
    try {
      List<String> children = nodesManager.getChildren(keyToBucketNumberPath);
      if (children != null) {
        for (String child : children) {
          String keyPath = keyToBucketNumberPath + File.separatorChar + child;
          List<String> keyChildren = nodesManager.getChildren(keyPath);
          Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = new HashMap<>();
          for (String keyChild : keyChildren) {
            String bucketPath =
                keyPath + File.separatorChar + keyChild + File.separatorChar
                    + ZkNodeName.BUCKET.getName();
            List<String> bucketChildren = nodesManager.getChildren(bucketPath);
            Bucket<KeyValueFrequency> bucket = createBucket(bucketChildren);

            String nodePath =
                keyPath + File.separatorChar + keyChild + File.separatorChar
                    + ZkNodeName.NODE.getName();
            List<String> nodeChildren = nodesManager.getChildren(nodePath);
            Node node = createNode(nodeChildren);
            bucketToNodeMap.put(bucket, node);
          }
          bucketToNodeNumberMap.put(child, bucketToNodeMap);
        }
      }

    } catch (KeeperException | InterruptedException e) {
      LOGGER.error(e.getMessage());
    }
    return bucketToNodeNumberMap;
  }

  /**
   * @return Map<String, Map<Object, Bucket<KeyValueFrequency>>>
   */
  public Map<String, Map<Object, Bucket<KeyValueFrequency>>> readKeyToValueToBucketMap() {
    KeyToValueToBucketPath =
        NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting()
            .getShardingTablePath()
            + File.separatorChar + ZkNodeName.KEY_TO_VALUE_TO_BUCKET.getName();
    try {
      List<String> children = nodesManager.getChildren(KeyToValueToBucketPath);
      for (String child : children) {
        String keyPath = KeyToValueToBucketPath + File.separatorChar + child;
        List<String> keyChildren = nodesManager.getChildren(keyPath);
        Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = new HashMap<>();
        for (String keyChild : keyChildren) {
          String bucketPath =
              keyPath + File.separatorChar + keyChild + File.separatorChar
                  + ZkNodeName.BUCKET.getName();
          List<String> bucketChildren = nodesManager.getChildren(bucketPath);
          Bucket<KeyValueFrequency> bucket = createBucket(bucketChildren);
          valueToBucketMap.put(keyChild, bucket);
        }
        keyToValueToBucketMap.put(child, valueToBucketMap);
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.error(e.getMessage());
    }
    return keyToValueToBucketMap;
  }

  /**
   * @param bucketDetails
   * @return Bucket<KeyValueFrequency>
   */
  private Bucket<KeyValueFrequency> createBucket(List<String> bucketDetails) {
    int bucketId = 0;
    long bucketSize = 0;
    for (String instanceProperty : bucketDetails) {
      String[] info = instanceProperty.split(ZkNodeName.EQUAL.getName());
      if (info[0].equalsIgnoreCase("id")) {
        bucketId = Integer.parseInt(info[1]);
      } else if (info[0].equalsIgnoreCase("size")) {
        bucketSize = Long.parseLong(info[1]);
      }
    }
    Bucket<KeyValueFrequency> bucket = new Bucket<>(bucketId, bucketSize);
    return bucket;
  }

  /**
   * @param nodeDetails
   * @return Node
   */
  private Node createNode(List<String> nodeDetails) {
    int nodeId = 0;
    long nodeCapacity = 0;
    for (String instanceProperty : nodeDetails) {
      String[] info = instanceProperty.split(ZkNodeName.EQUAL.getName());
      if (info[0].equalsIgnoreCase("nodeId")) {
        nodeId = Integer.parseInt(info[1]);
      } else if (info[0].equalsIgnoreCase("nodeCapacity")) {
        nodeCapacity = Long.parseLong(info[1]);
      }
    }
    Node node = new Node(nodeCapacity, nodeId);
    return node;
  }

}
