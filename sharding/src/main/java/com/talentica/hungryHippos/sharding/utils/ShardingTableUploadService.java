/**
 * 
 */
package com.talentica.hungryHippos.sharding.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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
  private static Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap;

  private static Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap;

  private static Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;

  private static Property<ZkProperty> zkproperty = CoordinationApplicationContext.getZkProperty();

  private static String zkKeyToBucketPath;
  private static String zkNodes;
  private static int BucketCombinationId;

  static {
    try (ObjectInputStream inKeyValueNodeNumberMap =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.keyToValueToBucketMapFile))) {
      keyToValueToBucketMap =
          (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) inKeyValueNodeNumberMap
              .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read keyValueNodeNumberMap. Please put the file in current directory");
    }

    try (ObjectInputStream bucketToNodeNumberMapInputStream =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.bucketToNodeNumberMapFile))) {
      bucketToNodeNumberMap =
          (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) bucketToNodeNumberMapInputStream
              .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read bucketToNodeNumberMap. Please put the file in current directory");
    }

    try (ObjectInputStream bucketToNodeNumberMapInputStream =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.bucketToNodeNumberMapFile))) {
      bucketToNodeNumberMap =
          (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) bucketToNodeNumberMapInputStream
              .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read bucketToNodeNumberMap. Please put the file in current directory");
    }

    try (ObjectInputStream bucketToNodeNumberMapInputStream =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.bucketCombinationToNodeNumbersMapFile))) {
      bucketCombinationToNodeNumbersMap =
          (Map<BucketCombination, Set<Node>>) bucketToNodeNumberMapInputStream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER
          .info("Unable to read bucketCombinationToNodeNumbersMap. Please put the file in current directory");
    }

  }

  public static void zkUploadBucketCombinationToNodeNumbersMap() throws IOException,
      InterruptedException {
    // CountDownLatch signal = new CountDownLatch(1);
    int bucketCombinationId = 0;
    String baseConfigPath =
        zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
            + ZkNodeName.BUCKET_COMBINATION + File.separatorChar;

    zkKeyToBucketPath =
        baseConfigPath + (bucketCombinationId) + File.separatorChar + ZkNodeName.KEY_TO_BUCKET;

    zkNodes = baseConfigPath + (bucketCombinationId) + File.separatorChar + ZkNodeName.NODES;

    for (Entry<BucketCombination, Set<Node>> entry : bucketCombinationToNodeNumbersMap.entrySet()) {
      Map<String, Bucket<KeyValueFrequency>> bucketCombinationMap =
          entry.getKey().getBucketsCombination();
      Set<Node> nodes = entry.getValue();
      for (Entry<String, Bucket<KeyValueFrequency>> bucketCombination : bucketCombinationMap
          .entrySet()) {
        String leafKeyPath =
            zkKeyToBucketPath + File.separatorChar + bucketCombination.getKey()
                + File.separatorChar + bucketCombination.getValue().getId();
        nodesManager.createPersistentNode(leafKeyPath, null);
      }

      for (Node node : nodes) {
        String leafNodePath = zkNodes + File.separatorChar + node.getNodeId();
        nodesManager.createPersistentNode(leafNodePath, null);
      }
      bucketCombinationId++;
      break;
    }
    // signal.await();
  }

  private Map<BucketCombination, Set<Node>> getBucketCombinationToNodeNumbersMap() {
    return bucketCombinationToNodeNumbersMap;
  }

  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
    return keyToValueToBucketMap;
  }

  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    return bucketToNodeNumberMap;
  }
}
