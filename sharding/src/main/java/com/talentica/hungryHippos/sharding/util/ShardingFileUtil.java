package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCombinationMap;
import com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCombinationToNodeNumberMap;
import com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCominationToNodeNumber;
import com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.BucketToNode;
import com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.BucketToNodeNumberMap;
import com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.Map_Type;
import com.talentica.hungryHippos.sharding.protos.KeyToValueToBucketMapProtos.KeyToValueToBucket;
import com.talentica.hungryHippos.sharding.protos.KeyToValueToBucketMapProtos.KeyToValueToBucketMap;
import com.talentica.hungryHippos.sharding.protos.KeyToValueToBucketMapProtos.ValueToBucketMap;
import com.talentica.hungryhippos.config.sharding.Column;

public class ShardingFileUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(ShardingFileUtil.class);

  public static void dumpKeyToValueToBucketFileOnDisk(String fileName,
      Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucket, String folderPath) {
    new File(folderPath).mkdirs();
    String filePath = (folderPath.endsWith(String.valueOf("/")) ? folderPath + fileName
        : folderPath + "/" + fileName);
    KeyToValueToBucketMap keyToValueToBucketProtos =
        keyToValueToBucketToProtoBufObject(keyToValueToBucket);
    try {
      FileOutputStream output = new FileOutputStream(filePath);
      keyToValueToBucketProtos.writeTo(output);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void dumpBucketToNodeNumberFileOnDisk(String fileName,
      Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumber, String folderPath) {
    new File(folderPath).mkdirs();
    String filePath = (folderPath.endsWith(String.valueOf("/")) ? folderPath + fileName
        : folderPath + "/" + fileName);
    BucketToNodeNumberMap bucketToNodeNumberMapProtos =
        bucketToNodeNumberToProtoBufObject(bucketToNodeNumber);
    try {
      FileOutputStream output = new FileOutputStream(filePath);
      bucketToNodeNumberMapProtos.writeTo(output);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void dumpBucketCombinationToNodeNumberFileOnDisk(String fileName,
      Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap, String folderPath) {
    new File(folderPath).mkdirs();
    String filePath = (folderPath.endsWith(String.valueOf("/")) ? folderPath + fileName
        : folderPath + "/" + fileName);
    BucketCombinationToNodeNumberMap bucketCombinationToNodeNumberMapProtos =
        bucketCombinationToNodeNumberToProtoBufObject(bucketCombinationToNodeNumberMap);
    try {
      FileOutputStream output = new FileOutputStream(filePath);
      bucketCombinationToNodeNumberMapProtos.writeTo(output);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static KeyToValueToBucketMap keyToValueToBucketToProtoBufObject(
      Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucket) {
    KeyToValueToBucketMap.Builder keyToValueToBucketMapProtos = KeyToValueToBucketMap.newBuilder();
    Set<String> keys = keyToValueToBucket.keySet();
    for (String key : keys) {
      KeyToValueToBucket.Builder keyToValueToBucketProtos = KeyToValueToBucket.newBuilder();
      keyToValueToBucketProtos.setKey(key);
      Map<Object, Bucket<KeyValueFrequency>> valueToBucket = keyToValueToBucket.get(key);
      Set<Object> keyValue = valueToBucket.keySet();
      for (Object obj : keyValue) {
        ValueToBucketMap.Builder valueToBucketMapProtos = ValueToBucketMap.newBuilder();
        valueToBucketMapProtos.setKeyValue(obj.toString());
        Bucket<KeyValueFrequency> bucket = valueToBucket.get(obj);
        com.talentica.hungryHippos.sharding.protos.KeyToValueToBucketMapProtos.ValueToBucketMap.Bucket.Builder bucketProto =
            com.talentica.hungryHippos.sharding.protos.KeyToValueToBucketMapProtos.ValueToBucketMap.Bucket
                .newBuilder();
        bucketProto.setId(bucket.getId());
        bucketProto.setSize(bucket.getSize());
        valueToBucketMapProtos.setBucket(bucketProto.build());
        keyToValueToBucketProtos.addValueToBucketMap(valueToBucketMapProtos.build());
      }

      keyToValueToBucketMapProtos.addKeyToValueToBucket(keyToValueToBucketProtos.build());
    }
    return keyToValueToBucketMapProtos.build();
  }

  public static Map<String, Map<Object, Bucket<KeyValueFrequency>>> readFromFileKeyToValueToBucket(
      String filePath, Map<String,String> dataTypeMap) {
    LOGGER.info(" sharding filePath : " + filePath);

    Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = new HashMap<>();
    try {
      KeyToValueToBucketMap keyToValueToBucketMapProtos =
          KeyToValueToBucketMap.parseFrom(new FileInputStream(filePath));
      for (KeyToValueToBucket keyToValueToBucketProtos : keyToValueToBucketMapProtos
          .getKeyToValueToBucketList()) {
        Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = new HashMap<>();
        for (ValueToBucketMap valueToBucketMapProtos : keyToValueToBucketProtos
            .getValueToBucketMapList()) {
          com.talentica.hungryHippos.sharding.protos.KeyToValueToBucketMapProtos.ValueToBucketMap.Bucket bucketProtos =
              valueToBucketMapProtos.getBucket();
          Bucket<KeyValueFrequency> bucket =
              new Bucket<>(bucketProtos.getId(), bucketProtos.getSize());
          String type = dataTypeMap.get(keyToValueToBucketProtos.getKey());
          Object obj = parseDataType(valueToBucketMapProtos.getKeyValue(), type);
          valueToBucketMap.put(obj, bucket);
        }
        keyToValueToBucketMap.put(keyToValueToBucketProtos.getKey(), valueToBucketMap);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return keyToValueToBucketMap;
  }

  private static BucketToNodeNumberMap bucketToNodeNumberToProtoBufObject(
      Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumber) {
    BucketToNodeNumberMap.Builder bucketToNodeNumberMapProtos = BucketToNodeNumberMap.newBuilder();
    Set<String> keys = bucketToNodeNumber.keySet();
    for (String key : keys) {
      BucketToNode.Builder bucketToNodeProtos = BucketToNode.newBuilder();
      bucketToNodeProtos.setKey(key);
      Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = bucketToNodeNumber.get(key);
      Set<Bucket<KeyValueFrequency>> buckets = bucketToNodeMap.keySet();
      for (Bucket<KeyValueFrequency> bucket : buckets) {
        Map_Type.Builder mapTypeProtos = Map_Type.newBuilder();
        com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.Map_Type.Bucket.Builder bucketProto =
            com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.Map_Type.Bucket
                .newBuilder();
        bucketProto.setId(bucket.getId());
        bucketProto.setSize(bucket.getSize());
        mapTypeProtos.setBucket(bucketProto.build());
        Node node = bucketToNodeMap.get(bucket);
        com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.Map_Type.Node.Builder nodeProto =
            com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.Map_Type.Node
                .newBuilder();
        nodeProto.setNodeId(node.getNodeId());
        nodeProto.setNodeCapacity(node.getNodeCapacity());
        nodeProto.setRemainingCapacity(node.getRemainingCapacity());
        mapTypeProtos.setNode(nodeProto);
        bucketToNodeProtos.addBucketToNode(mapTypeProtos.build());
      }

      bucketToNodeNumberMapProtos.addBucketToNodeMap(bucketToNodeProtos.build());
    }
    return bucketToNodeNumberMapProtos.build();
  }

  public static Map<String, Map<Bucket<KeyValueFrequency>, Node>> readFromFileBucketToNodeNumber(
      String filePath) {
    LOGGER.info(" sharding filePath : " + filePath);
    Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();
    try {
      BucketToNodeNumberMap bucketToNodeNumberMapProtos =
          BucketToNodeNumberMap.parseFrom(new FileInputStream(filePath));
      for (BucketToNode bucketToNodeNumberProtos : bucketToNodeNumberMapProtos
          .getBucketToNodeMapList()) {
        Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = new HashMap<>();
        for (Map_Type mapTypeProtos : bucketToNodeNumberProtos.getBucketToNodeList()) {
          com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.Map_Type.Bucket bucketProtos =
              mapTypeProtos.getBucket();
          Bucket<KeyValueFrequency> bucket =
              new Bucket<>(bucketProtos.getId(), bucketProtos.getSize());
          com.talentica.hungryHippos.sharding.protos.BucketToNodeNumberProtos.Map_Type.Node nodeProtos =
              mapTypeProtos.getNode();
          Node node = new Node(nodeProtos.getNodeCapacity(), nodeProtos.getNodeId());
          bucketToNodeMap.put(bucket, node);
        }
        bucketToNodeNumberMap.put(bucketToNodeNumberProtos.getKey(), bucketToNodeMap);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return bucketToNodeNumberMap;
  }

  private static BucketCombinationToNodeNumberMap bucketCombinationToNodeNumberToProtoBufObject(
      Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap) {
    BucketCombinationToNodeNumberMap.Builder bucketCombinationToNodeNumberMapProtos =
        BucketCombinationToNodeNumberMap.newBuilder();
    Set<BucketCombination> bucketCombinationKeys = bucketCombinationToNodeNumberMap.keySet();
    for (BucketCombination bucketCombination : bucketCombinationKeys) {
      Map<String, Bucket<KeyValueFrequency>> bucketsCombination =
          bucketCombination.getBucketsCombination();
      BucketCominationToNodeNumber.Builder bucketCominationToNodeNumber =
          BucketCominationToNodeNumber.newBuilder();
      Set<String> keys = bucketsCombination.keySet();
      BucketCombinationMap.Builder bucketCombinationMapProtos = BucketCombinationMap.newBuilder();
      for (String key : keys) {

        Bucket<KeyValueFrequency> bucket = bucketsCombination.get(key);
        com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCombination.Builder bucketCombinationProtos =
            com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCombination
                .newBuilder();
        bucketCombinationProtos.setKey(key);
        com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCombination.Bucket.Builder bucketProtos =
            com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCombination.Bucket
                .newBuilder();
        bucketProtos.setId(bucket.getId());
        bucketProtos.setSize(bucket.getSize());
        bucketCombinationProtos.setBucket(bucketProtos.build());
        bucketCombinationMapProtos.addBucketCombination(bucketCombinationProtos.build());

      }
      bucketCominationToNodeNumber.setBucketCombinationMap(bucketCombinationMapProtos.build());
      Set<Node> nodes = bucketCombinationToNodeNumberMap.get(bucketCombination);
      for (Node node : nodes) {
        com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.Node.Builder nodeProtos =
            com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.Node
                .newBuilder();
        nodeProtos.setNodeId(node.getNodeId());
        nodeProtos.setNodeCapacity(node.getNodeCapacity());
        bucketCominationToNodeNumber.addNodes(nodeProtos.build());
      }
      bucketCombinationToNodeNumberMapProtos
          .addBucketCombinationToNodeNumber(bucketCominationToNodeNumber.build());
    }
    return bucketCombinationToNodeNumberMapProtos.build();
  }

  public static Map<BucketCombination, Set<Node>> readFromFileBucketCombinationToNodeNumber(
      String filePath) {
    LOGGER.info(" sharding filePath : " + filePath);
    Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap = new HashMap<>();

    try {
      BucketCombinationToNodeNumberMap bucketCombinationToNodeNumberMapProtos =
          BucketCombinationToNodeNumberMap.parseFrom(new FileInputStream(filePath));
      for (BucketCominationToNodeNumber bucketCominationToNodeNumberProtos : bucketCombinationToNodeNumberMapProtos
          .getBucketCombinationToNodeNumberList()) {
        Map<String, Bucket<KeyValueFrequency>> bucketCombinationMap = new HashMap<>();
        BucketCombinationMap bucketCombinationMapProtos =
            bucketCominationToNodeNumberProtos.getBucketCombinationMap();
        for (com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCombination bucketCombinationProtos : bucketCombinationMapProtos
            .getBucketCombinationList()) {
          com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.BucketCombination.Bucket bucketProtos =
              bucketCombinationProtos.getBucket();
          Bucket<KeyValueFrequency> bucket =
              new Bucket<>(bucketProtos.getId(), bucketProtos.getSize());
          bucketCombinationMap.put(bucketCombinationProtos.getKey(), bucket);
        }
        BucketCombination bucketCombination = new BucketCombination(bucketCombinationMap);
        Set<Node> nodes = new LinkedHashSet<>();
        for (com.talentica.hungryHippos.sharding.protos.BucketCombinationToNodeNumberMapProtos.Node nodeProtos : bucketCominationToNodeNumberProtos
            .getNodesList()) {
          Node node = new Node(nodeProtos.getNodeCapacity(), nodeProtos.getNodeId());
          nodes.add(node);
        }
        bucketCombinationToNodeNumberMap.put(bucketCombination, nodes);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return bucketCombinationToNodeNumberMap;
  }
  
  public static Map<String, String> getDataTypeMap(ShardingApplicationContext context) {
    Map<String, String> dataTypeMap = new HashMap<>();
    List<Column> columns =
        context.getShardingClientConfig().getInput().getDataDescription().getColumn();
    for (Column column : columns) {
      dataTypeMap.put(column.getName(), column.getDataType());
    }
    return dataTypeMap;
  }

  public static Object parseDataType(String value, String type) {
    switch (type) {
      case ("INT"):
        return Integer.valueOf(value);
      case ("FLOAT"):
        return Float.valueOf(value);
      case ("DOUBLE"):
        return Double.valueOf(value);
      case ("LONG"):
        return Long.valueOf(value);
      default:
        return value;
    }
  }

}
