package com.talentica.hungryHippos.sharding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.utility.MapUtils;

/**
 * Created by debasishc on 14/8/15.
 */
public class Sharding {

  private static final Logger logger = LoggerFactory.getLogger(Sharding.class);

  // Map<key1,{KeyValueFrequency(value1,10),KeyValueFrequency(value2,11)}>
  private Map<String, List<Bucket<KeyValueFrequency>>> keysToListOfBucketsMap = new HashMap<>();
  private Map<String, Map<MutableCharArrayString, Long>> keyValueFrequencyMap = new HashMap<>();

  private Map<String, Integer> keyToIndexMap = new HashMap<>();

  // Map<Key1,Map<value1,Node(1)>
  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();
  PriorityQueue<Node> fillupQueue = new PriorityQueue<>(new NodeRemainingCapacityComparator());

  // e.g. Map<KeyCombination({key1,value1},{key2,value2},{key3,value3}),count>
  private Map<BucketCombination, Long> bucketCombinationFrequencyMap = new HashMap<>();
  private Map<Node, List<BucketCombination>> nodeToKeyMap = new HashMap<>();
  private Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap = new HashMap<>();
  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<>();

  public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";
  public final static String bucketCombinationToNodeNumbersMapFile =
      "bucketCombinationToNodeNumbersMap";
  public final static String keyToValueToBucketMapFile = "keyToValueToBucketMap";
  private final static String BAD_RECORDS_FILE = CoordinationApplicationContext.getProperty()
      .getValueByKey("common.bad.records.file.out") + "_sharding.err";

  public Sharding(int numNodes) {
    for (int i = 0; i < numNodes; i++) {
      Node node = new Node(300000, i);
      fillupQueue.offer(node);
      nodeToKeyMap.put(node, new ArrayList<BucketCombination>());
    }
  }

  public static void doSharding(Reader input) {
    logger.info("SHARDING STARTED");
    Sharding sharding = new Sharding(CoordinationApplicationContext.getTotalNumberOfNodes());
    try {
      sharding.populateFrequencyFromData(input);
      sharding.populateKeysToListOfBucketsMap();
      sharding.updateBucketToNodeNumbersMap(input);
      sharding.shardAllKeys();
      CommonUtil.dumpFileOnDisk(Sharding.bucketCombinationToNodeNumbersMapFile,
          sharding.bucketCombinationToNodeNumbersMap);
      CommonUtil.dumpFileOnDisk(Sharding.bucketToNodeNumberMapFile, sharding.bucketToNodeNumberMap);
      CommonUtil.dumpFileOnDisk(Sharding.keyToValueToBucketMapFile, sharding.keyToValueToBucketMap);
      if (logger.isDebugEnabled()) {
        logger.debug("keyToValueToBucketMap:"
            + MapUtils.getFormattedString(sharding.keyToValueToBucketMap));
        logger.debug("bucketCombinationToNodeNumbersMap: "
            + MapUtils.getFormattedString(sharding.bucketCombinationToNodeNumbersMap));
      }

    } catch (IOException | NodeOverflowException e) {
      logger.error("Error occurred during sharding process.", e);
    }
  }

  private void updateBucketToNodeNumbersMap(Reader data) throws IOException {

    logger.info("Calculating buckets to node numbers map started");

    String[] keys = CoordinationApplicationContext.getShardingDimensions();
    // Map<key1,Map<value1,count>>
    while (true) {
      DataTypes[] parts = null;
      try {
        parts = data.read();
      } catch (InvalidRowException e) {
        continue;
      }
      if (parts == null) {
        data.close();
        break;
      }
      MutableCharArrayString[] values = new MutableCharArrayString[keys.length];
      Map<String, Bucket<KeyValueFrequency>> bucketCombinationMap = new HashMap<>();
      for (int i = 0; i < keys.length; i++) {
        String key = keys[i];
        int keyIndex = keyToIndexMap.get(key);
        values[i] = (MutableCharArrayString) parts[keyIndex].clone();
        Bucket<KeyValueFrequency> bucket = keyToValueToBucketMap.get(key).get(values[i]);
        bucketCombinationMap.put(key, bucket);
      }
      BucketCombination keyCombination = new BucketCombination(bucketCombinationMap);
      Long count = bucketCombinationFrequencyMap.get(keyCombination);
      if (count == null) {
        bucketCombinationFrequencyMap.put(keyCombination, 1L);
      } else {
        bucketCombinationFrequencyMap.put(keyCombination, count + 1);
      }
    }
    logger.info("Calculating buckets to node numbers map finished");
  }

  private void setKeysToIndexes() {
    String[] keys = CoordinationApplicationContext.getColumnsConfiguration();
    int index = 0;
    for (String key : keys) {
      keyToIndexMap.put(key, index);
      index++;
    }
  }

  // TODO: This method needs to be generalized
  Map<String, Map<MutableCharArrayString, Long>> populateFrequencyFromData(Reader data)
      throws IOException {

    logger.info("Populating frequency map from data started");
    setKeysToIndexes();
    String[] keys = CoordinationApplicationContext.getShardingDimensions();
    int lineNo = 0;
    FileWriter.openFile(BAD_RECORDS_FILE);
    int count = 0;
    while (true) {
      DataTypes[] parts = null;
      try {
        parts = data.read();
      } catch (InvalidRowException e) {
        FileWriter.flushData(lineNo++, e);
        continue;
      }
      if (parts == null) {
        break;
      }
      MutableCharArrayString[] values = new MutableCharArrayString[keys.length];

      for (int i = 0; i < keys.length; i++) {
        String key = keys[i];
        int keyIndex = keyToIndexMap.get(key);
        values[i] = (MutableCharArrayString) parts[keyIndex].clone();
        Map<MutableCharArrayString, Long> frequencyPerValue = keyValueFrequencyMap.get(key);
        if (frequencyPerValue == null) {
          frequencyPerValue = new HashMap<>();
          keyValueFrequencyMap.put(key, frequencyPerValue);
        }
        Long frequency = frequencyPerValue.get(values[i]);
        if (frequency == null) {
          frequency = 0L;
        }
        frequencyPerValue.put(values[i], frequency + 1);
      }
    }

    logger.info("Populating frequency map from data finished");

    FileWriter.close();

    return keyValueFrequencyMap;
  }

  private Map<String, List<Bucket<KeyValueFrequency>>> populateKeysToListOfBucketsMap() {
    logger.info("Calculating keys to list of buckets map started");
    String[] keys = CoordinationApplicationContext.getShardingDimensions();
    int totalNoOfBuckets = BucketsCalculator.calculateNumberOfBucketsNeeded();
    logger.info("Total no. of buckets: {}", totalNoOfBuckets);
    Map<String, List<KeyValueFrequency>> keyToListOfKeyValueFrequency =
        getSortedKeyToListOfKeyValueFrequenciesMap();
    for (int i = 0; i < keys.length; i++) {
      Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = new HashMap<>();
      keyToValueToBucketMap.put(keys[i], valueToBucketMap);
      long frequencyOfAlreadyAddedValues = 0;
      int bucketCount = 0;
      Map<MutableCharArrayString, Long> frequencyPerValue = keyValueFrequencyMap.get(keys[i]);
      long idealAverageSizeOfOneBucket = getSizeOfOneBucket(frequencyPerValue, totalNoOfBuckets);
      logger.info("Ideal size of bucket for {}:{}", new Object[] {keys[i],
          idealAverageSizeOfOneBucket});
      Bucket<KeyValueFrequency> bucket = new Bucket<>(bucketCount, idealAverageSizeOfOneBucket);
      List<Bucket<KeyValueFrequency>> buckets = new ArrayList<>();
      buckets.add(bucket);
      List<KeyValueFrequency> sortedKeyValueFrequencies = keyToListOfKeyValueFrequency.get(keys[i]);
      if (!sortedKeyValueFrequencies.isEmpty()) {
        for (KeyValueFrequency keyValueFrequency : sortedKeyValueFrequencies) {
          long sizeOfCurrentBucket = idealAverageSizeOfOneBucket;
          Long frequency = keyValueFrequency.getFrequency();
          if (frequency > idealAverageSizeOfOneBucket) {
            sizeOfCurrentBucket = frequency;
            logger
                .info(
                    "Frequency of key {} value {} exceeded ideal size of bucket, so bucket size is: {}",
                    new Object[] {keys[i], keyValueFrequency.getKeyValue(), sizeOfCurrentBucket});
          }
          if (frequencyOfAlreadyAddedValues + frequency > idealAverageSizeOfOneBucket) {
            bucket = new Bucket<>(++bucketCount, sizeOfCurrentBucket);
            buckets.add(bucket);
            frequencyOfAlreadyAddedValues = 0;
          }
          frequencyOfAlreadyAddedValues = frequencyOfAlreadyAddedValues + frequency;
          bucket.add(keyValueFrequency);
          valueToBucketMap.put(keyValueFrequency.getKeyValue(), bucket);
        }
        this.keysToListOfBucketsMap.put(keys[i], buckets);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("keyValueFrequencyMap: " + MapUtils.getFormattedString(keyValueFrequencyMap));
    }
    logger.info("Calculating keys to list of buckets map finished");
    return this.keysToListOfBucketsMap;
  }

  public Map<String, List<KeyValueFrequency>> getSortedKeyToListOfKeyValueFrequenciesMap() {
    Map<String, List<KeyValueFrequency>> keyToListOfKeyValueFrequency = new HashMap<>();
    String[] keys = CoordinationApplicationContext.getShardingDimensions();
    for (String key : keys) {
      List<KeyValueFrequency> frequencies = new ArrayList<>();
      Map<MutableCharArrayString, Long> keyValueToFrequencyMap = keyValueFrequencyMap.get(key);
      for (MutableCharArrayString keyValue : keyValueToFrequencyMap.keySet()) {
        frequencies.add(new KeyValueFrequency(keyValue, keyValueToFrequencyMap.get(keyValue)));
      }
      Collections.sort(frequencies);
      keyToListOfKeyValueFrequency.put(key, frequencies);
    }
    return keyToListOfKeyValueFrequency;
  }

  private long getSizeOfOneBucket(Map<MutableCharArrayString, Long> frequencyPerValue,
      int noOfBuckets) {
    long sizeOfOneBucket = 0;
    long totalofAllKeyValueFrequencies = 0;
    for (MutableCharArrayString mutableCharArrayString : frequencyPerValue.keySet()) {
      totalofAllKeyValueFrequencies =
          totalofAllKeyValueFrequencies + frequencyPerValue.get(mutableCharArrayString);
      sizeOfOneBucket = totalofAllKeyValueFrequencies / (noOfBuckets - 1);
    }
    return sizeOfOneBucket;
  }

  private void shardSingleKey(String keyName) throws NodeOverflowException {
    List<Bucket<KeyValueFrequency>> buckets = keysToListOfBucketsMap.get(keyName);
    Map<Bucket<KeyValueFrequency>, Node> bucketToNodeNumber = new HashMap<>();
    bucketToNodeNumberMap.put(keyName, bucketToNodeNumber);
    Collections.sort(buckets);
    int counter = 0;
    for (Bucket<KeyValueFrequency> bucket : buckets) {
      counter++;
      if (counter % 100 == 0) {
        logger.info("Buckets processed: {}", counter);
      }
      Node mostEmptyNode = fillupQueue.poll();
      List<BucketCombination> currentKeys = nodeToKeyMap.get(mostEmptyNode);
      if (currentKeys == null) {
        currentKeys = new ArrayList<>();
        nodeToKeyMap.put(mostEmptyNode, currentKeys);
      }
      long currentSize = sumForKeyCombinationUnion(currentKeys);
      Map<String, Bucket<KeyValueFrequency>> wouldBeMap = new HashMap<>();
      wouldBeMap.put(keyName, bucket);
      currentKeys.add(new BucketCombination(wouldBeMap));
      long wouldBeSize = sumForKeyCombinationUnion(currentKeys);
      mostEmptyNode.fillUpBy(wouldBeSize - currentSize);
      fillupQueue.offer(mostEmptyNode);
      bucketToNodeNumber.put(bucket, mostEmptyNode);
    }
  }

  private long sumForKeyCombinationUnion(List<BucketCombination> keyCombination) {
    long sum = 0;
    for (Map.Entry<BucketCombination, Long> entry : bucketCombinationFrequencyMap.entrySet()) {
      BucketCombination keyCombination1 = entry.getKey();
      if (keyCombination1.checkMatchOr(keyCombination)) {
        sum += entry.getValue();
      }
    }
    return sum;
  }

  public void shardAllKeys() throws NodeOverflowException {
    for (String key : keysToListOfBucketsMap.keySet()) {
      logger.info("Sharding on key started: {}", key);
      shardSingleKey(key);
      logger.info("Sharding on key finished: {}", key);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("bucketToNodeNumberMap:" + MapUtils.getFormattedString(bucketToNodeNumberMap));
    }
    List<String> keyNameList = new ArrayList<>();
    keyNameList.addAll(keysToListOfBucketsMap.keySet());
    logger.info("Generating table started");
    makeShardingTable(new BucketCombination(new HashMap<String, Bucket<KeyValueFrequency>>()),
        keyNameList);
    logger.info("Generating table finished");
  }

  private void makeShardingTable(BucketCombination source, List<String> keyNames)
      throws NodeOverflowException {
    String keyName;
    if (keyNames.size() >= 1) {
      keyName = keyNames.get(0);
    } else {
      keyName = null;
    }
    if (keyName == null) {
      // exit case
      // lets check which nodes it goes.
      Set<Node> nodesForBucketCombination = new HashSet<>();
      for (Map.Entry<String, Bucket<KeyValueFrequency>> buckets : source.getBucketsCombination()
          .entrySet()) {
        Node n = bucketToNodeNumberMap.get(buckets.getKey()).get(buckets.getValue());
        if (n != null) {
          nodesForBucketCombination.add(n);
        }
      }
      int numberOfIntersectionStorage =
          source.getBucketsCombination().size() - nodesForBucketCombination.size();
      Set<Node> nodesToPutBack = new HashSet<>();
      while (numberOfIntersectionStorage > 0) {
        Node mostEmptyNode = fillupQueue.poll();
        if (!nodesForBucketCombination.contains(mostEmptyNode)) {
          nodesForBucketCombination.add(mostEmptyNode);
          Long value = bucketCombinationFrequencyMap.get(source);
          if (value == null) {
            value = 0l;
          }
          mostEmptyNode.fillUpBy(value);
          numberOfIntersectionStorage--;
        }
        nodesToPutBack.add(mostEmptyNode);
      }
      nodesToPutBack.forEach(n -> fillupQueue.offer(n));
      bucketCombinationToNodeNumbersMap.put(source, nodesForBucketCombination);
    } else {
      List<String> restOfKeyNames = new LinkedList<>();
      restOfKeyNames.addAll(keyNames);
      restOfKeyNames.remove(keyName);
      List<Bucket<KeyValueFrequency>> buckets = keysToListOfBucketsMap.get(keyName);
      for (Bucket<KeyValueFrequency> bucket : buckets) {
        Map<String, Bucket<KeyValueFrequency>> nextSourceMap = new HashMap<>();
        nextSourceMap.putAll(source.getBucketsCombination());
        BucketCombination nextSource = new BucketCombination(nextSourceMap);
        nextSource.getBucketsCombination().put(keyName, bucket);
        makeShardingTable(nextSource, restOfKeyNames);
      }
    }
  }

}
