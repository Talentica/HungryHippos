package com.talentica.hungryHippos.sharding.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
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

/**
 * 
 * @author sohanc
 *
 */
public class ShardingTableReadService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ShardingTableReadService.class.getClass());

  private NodesManager nodesManager;
  private Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap = new HashMap<>();
  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();
  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<>();
  private static Property<ZkProperty> zkproperty = CoordinationApplicationContext.getZkProperty();

  private static String bucketCombinationPath;
  private static String keyToBucketNumberPath;
  private static String KeyToValueToBucketPath;

  public ShardingTableReadService() throws FileNotFoundException, JAXBException {
    nodesManager = NodesManagerContext.getNodesManagerInstance();
  }

  public Map<BucketCombination, Set<Node>> readBucketCombinationToNodeNumbersMap() {
    bucketCombinationPath = zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
        + ZkNodeName.BUCKET_COMBINATION.getName();
    try {
      List<String> children = nodesManager.getChildren(bucketCombinationPath);
      if (children != null) {
        for (String child : children) {
          String keyToBucketMapPath = bucketCombinationPath + File.separatorChar + child
              + File.separatorChar + ZkNodeName.KEY_TO_BUCKET.getName();
          List<String> keyToBucketChildren = nodesManager.getChildren(keyToBucketMapPath);
          Map<String, Bucket<KeyValueFrequency>> keyToBucketMap =
              new HashMap<String, Bucket<KeyValueFrequency>>();
          for (String keyToBucketChild : keyToBucketChildren) {
            String bucketPath = keyToBucketMapPath + File.separatorChar + keyToBucketChild
                + File.separatorChar + ZkNodeName.BUCKET.getName();
            List<String> bucketChildren = nodesManager.getChildren(bucketPath);
            Bucket<KeyValueFrequency> bucket = createBucket(bucketChildren);
            keyToBucketMap.put(keyToBucketChild, bucket);
          }
          BucketCombination bucketCombination = new BucketCombination(keyToBucketMap);
          String nodesPath = bucketCombinationPath + File.separatorChar + child + File.separatorChar
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

  public Map<String, Map<Bucket<KeyValueFrequency>, Node>> readBucketToNodeNumberMap() {
    keyToBucketNumberPath = zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
        + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName();
    try {
      List<String> children = nodesManager.getChildren(keyToBucketNumberPath);
      if (children != null) {
        for (String child : children) {
          String keyPath = keyToBucketNumberPath + File.separatorChar + child;
          List<String> keyChildren = nodesManager.getChildren(keyPath);
          Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = new HashMap<>();
          for (String keyChild : keyChildren) {
            String bucketPath = keyPath + File.separatorChar + keyChild + File.separatorChar
                + ZkNodeName.BUCKET.getName();
            List<String> bucketChildren = nodesManager.getChildren(bucketPath);
            Bucket<KeyValueFrequency> bucket = createBucket(bucketChildren);

            String nodePath = keyPath + File.separatorChar + keyChild + File.separatorChar
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

  public Map<String, Map<Object, Bucket<KeyValueFrequency>>> readKeyToValueToBucketMap() {
    KeyToValueToBucketPath = zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
        + ZkNodeName.KEY_TO_VALUE_TO_BUCKET.getName();
    try {
      List<String> children = nodesManager.getChildren(KeyToValueToBucketPath);
      for (String child : children) {
        String keyPath = KeyToValueToBucketPath + File.separatorChar + child;
        List<String> keyChildren = nodesManager.getChildren(keyPath);
        Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = new HashMap<>();
        for (String keyChild : keyChildren) {
          String bucketPath = keyPath + File.separatorChar + keyChild + File.separatorChar
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
