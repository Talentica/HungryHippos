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

  private static String zkKeyToBucketPath;
  private static String zkNodes;
  private static int BucketCombinationId;


  public static void zkUploadBucketCombinationToNodeNumbersMap() throws IOException,
      InterruptedException, IllegalArgumentException, IllegalAccessException {

    int bucketCombinationId = 0;
    String baseConfigPath =
        zkproperty.getValueByKey("zookeeper.config_path") + File.separatorChar
            + ZkNodeName.BUCKET_COMBINATION.getName() + File.separatorChar;

    for (Entry<BucketCombination, Set<Node>> entry : getBucketCombinationToNodeNumbersMap()
        .entrySet()) {
      zkKeyToBucketPath =
          baseConfigPath
              + (ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + bucketCombinationId)
              + File.separatorChar + ZkNodeName.KEY_TO_BUCKET.getName();
      zkNodes =
          baseConfigPath
              + (ZkNodeName.ID.getName() + ZkNodeName.UNDERSCORE.getName() + bucketCombinationId)
              + File.separatorChar + ZkNodeName.NODES.getName();

      Map<String, Bucket<KeyValueFrequency>> bucketCombinationMap =
          entry.getKey().getBucketsCombination();
      Set<Node> nodes = entry.getValue();
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
          String leafNode = fieldName + "=" + value;
          String leafNodePath =
              zkKeyToBucketPath + File.separatorChar + key + File.separatorChar
                  + ZkNodeName.BUCKET.getName() + File.separatorChar + leafNode;
          nodesManager.createPersistentNode(leafNodePath, counter);
        }
        counter.await();
      }
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
          String leafNode = fieldName + "=" + value;
          String leafNodePath =
              zkNodes + File.separatorChar
                  + (ZkNodeName.NODE.getName() + ZkNodeName.UNDERSCORE.getName() + nodeId)
                  + File.separatorChar + leafNode;
          nodesManager.createPersistentNode(leafNodePath, counter);
        }
        nodeId++;
        counter.await();
      }
      bucketCombinationId++;
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
