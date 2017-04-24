/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.DataTypesFactory;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.utility.Counter;
import com.talentica.hungryHippos.utility.MapUtils;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;

/**
 * {@code Sharding} , used for doing sharding of an input file.
 * 
 * @author debasishc
 * @since 14/8/15.
 */
public class Sharding {

  private static final Logger logger = LoggerFactory.getLogger(Sharding.class);

  // Map<key1,{KeyValueFrequency(value1,10),KeyValueFrequency(value2,11)}>
  private HashMap<String, List<Bucket<KeyValueFrequency>>> keysToListOfBucketsMap = new HashMap<>();
  private HashMap<String, HashMap<DataTypes, Long>> keyValueFrequencyMap = new HashMap<>();
  private HashMap<String, HashMap<DataTypes, Long>> splittedKeyValueMap = new HashMap<>();

  private HashMap<String, Integer> keyToIndexMap = new HashMap<>();

  // Map<Key1,Map<value1,Node(1)>
  private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap =
      new HashMap<>();
  PriorityQueue<Node> fillupQueue = new PriorityQueue<>(new NodeRemainingCapacityComparator());

  // e.g. Map<KeyCombination({key1,value1},{key2,value2},{key3,value3}),count>
  private HashMap<BucketCombination, Long> bucketCombinationFrequencyMap = new HashMap<>();
  private HashMap<Node, List<BucketCombination>> nodeToKeyMap = new HashMap<>();
  private HashMap<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap = new HashMap<>();
  private HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<>();
  private ShardingApplicationContext context;

  private BucketsCalculator bucketsCalculator;
  private String[] keys;



  /**
   * creates a new instance of Sharding.
   * 
   * @param clusterConfig
   * @param context
   */
  public Sharding(ClusterConfig clusterConfig, ShardingApplicationContext context,
      float bucketCountWeight) {
    this.context = context;
    bucketsCalculator = new BucketsCalculator(context);
    keys = context.getShardingDimensions();
    List<com.talentica.hungryhippos.config.cluster.Node> clusterNodes = clusterConfig.getNode();
    for (com.talentica.hungryhippos.config.cluster.Node clusterNode : clusterNodes) {
      Node node = new Node(300000, clusterNode.getIdentifier(), bucketCountWeight);
      fillupQueue.offer(node);
      nodeToKeyMap.put(node, new ArrayList<BucketCombination>());
    }
  }

  /**
   * does sharding on specified Reader.
   * 
   * @param input
   * @throws ClassNotFoundException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws JAXBException
   */
  public void doSharding(Reader input)
      throws ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    logger.info("SHARDING STARTED");
    try {
      populateFrequencyFromData(input);
      populateKeysToListOfBucketsMap();
      updateBucketToNodeNumbersMap(input);
      shardAllKeys();
      if (logger.isDebugEnabled()) {
        logger.debug("keyToValueToBucketMap:" + MapUtils.getFormattedString(keyToValueToBucketMap));
        logger.debug("bucketCombinationToNodeNumbersMap: "
            + MapUtils.getFormattedString(bucketCombinationToNodeNumbersMap));
      }

    } catch (IOException | NodeOverflowException e) {
      logger.error("Error occurred during sharding process.", e);
    }
  }

  /**
   * dump the sharding table files that are created.
   * 
   * @param directoryPath
   * @param shardingClientConfigFilePath
   * @param shardingServerConfigFilePath
   * @throws IOException
   */
  public void dumpShardingTableFiles(String directoryPath, String shardingClientConfigFilePath,
      String shardingServerConfigFilePath) throws IOException {
    ShardingFileUtil.dumpBucketCombinationToNodeNumberFileOnDisk(
        ShardingApplicationContext.bucketCombinationToNodeNumbersMapFile,
        bucketCombinationToNodeNumbersMap, directoryPath);
    ShardingFileUtil.dumpBucketToNodeNumberFileOnDisk(
        ShardingApplicationContext.bucketToNodeNumberMapFile, bucketToNodeNumberMap, directoryPath);
    ShardingFileUtil.dumpKeyToValueToBucketFileOnDisk(
        ShardingApplicationContext.keyToValueToBucketMapFile, keyToValueToBucketMap, directoryPath);
    ShardingFileUtil.dumpSplittedKeyValueMapFileOnDisk(
        ShardingApplicationContext.splittedKeyValueMapFile, splittedKeyValueMap, directoryPath);
    FileUtils.writeStringToFile(
        new File(directoryPath + File.separator + "sharding-client-config.xml"),
        FileUtils.readFileToString(new File(shardingClientConfigFilePath), "UTF-8"), "UTF-8");
    FileUtils.writeStringToFile(
        new File(directoryPath + File.separator + "sharding-server-config.xml"),
        FileUtils.readFileToString(new File(shardingServerConfigFilePath), "UTF-8"), "UTF-8");
  }

  private void updateBucketToNodeNumbersMap(Reader data) throws IOException, ClassNotFoundException,
      KeeperException, InterruptedException, JAXBException {

    logger.info("Calculating buckets to node numbers map started");
    data.reset();
    String[] keys = context.getShardingDimensions();
    // Map<key1,Map<value1,count>>
    
    HashMap<String, HashMap<DataTypes, Counter>> splitKeyValueCounter = new HashMap<>();
    for(Map.Entry<String, HashMap<DataTypes, Long>> keyValueSplitEntry : splittedKeyValueMap.entrySet()){
      HashMap<DataTypes, Long> valueSplitCount = keyValueSplitEntry.getValue();
      HashMap<DataTypes, Counter> valueSplitCounter = new HashMap<>();
      for(Map.Entry<DataTypes, Long> valueSplitCountEntry : valueSplitCount.entrySet()){
        valueSplitCounter.put(valueSplitCountEntry.getKey(), new Counter(valueSplitCountEntry.getValue()-1));
      }
      splitKeyValueCounter.put(keyValueSplitEntry.getKey(), valueSplitCounter);
    }
  
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
      DataTypes[] values = new DataTypes[keys.length];
      HashMap<String, Bucket<KeyValueFrequency>> bucketCombinationMap = new HashMap<>();
      for (int i = 0; i < keys.length; i++) {
        String key = keys[i];
        int keyIndex = keyToIndexMap.get(key);
        values[i] = parts[keyIndex].clone();
        if(splitKeyValueCounter.get(key) != null && splitKeyValueCounter.get(key).get(values[i]) != null){
          values[i].setSplitIndex((int)splitKeyValueCounter.get(key).get(values[i]).getNextCount());
        }
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

  private void setKeysToIndexes() throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException {
    keyToIndexMap = context.getColumnsConfiguration();
  }

  /**
   * populate map with frequency of data occured in the reader.
   * 
   * @param data
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws JAXBException
   */
  // TODO: This method needs to be generalized
  HashMap<String, HashMap<DataTypes, Long>> populateFrequencyFromData(Reader data)
      throws IOException, ClassNotFoundException, KeeperException, InterruptedException,
      JAXBException {

    logger.info("Populating frequency map from data started");
    setKeysToIndexes();
    String[] keys = context.getShardingDimensions();
   
    int lineNo = 0;
    FileWriter fileWriter =
        new FileWriter(context.getShardingClientConfig().getBadRecordsFileOut() + "_sharding.err");
    fileWriter.openFile();
    while (true) {
      DataTypes[] parts = null;
      try {
        parts = data.read();
      } catch (InvalidRowException e) {
        fileWriter.flushData(lineNo++, e);
        continue;
      }
      if (parts == null) {
        break;
      }
      DataTypes[] values = new DataTypes[keys.length];

      for (int i = 0; i < keys.length; i++) {
        String key = keys[i];
        int keyIndex = keyToIndexMap.get(key);
        values[i] = parts[keyIndex].clone();
        HashMap<DataTypes, Long> frequencyPerValue = keyValueFrequencyMap.get(key);
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

    fileWriter.close();
    
    splitKey();

    return keyValueFrequencyMap;
  }
  
  private void splitKey(){
    HashMap<String, List<KeyValueFrequency>> keyToListOfKeyValueFrequency =
            getSortedKeyToListOfKeyValueFrequenciesMap();
    for(String key : keyToListOfKeyValueFrequency.keySet()){
        List<KeyValueFrequency> keyValueFrequenciesList = keyToListOfKeyValueFrequency.get(key);
        HashMap<DataTypes, Long> valueSplitMap = new HashMap<>();
        double avg = calculateAvgFrequency(key);
        double threshold = context.getMaxSkew() * avg;
        for(int i = 0; i < keyValueFrequenciesList.size(); i++){
            KeyValueFrequency keyValueFrequency = keyValueFrequenciesList.get(i);
            if(keyValueFrequency.getFrequency() <= threshold){
                break;
            }else{
                long numberOfSplit = (long)Math.ceil(keyValueFrequency.getFrequency() / threshold);
                DataTypes splittedKey = (DataTypes)keyValueFrequency.getKeyValue();
                valueSplitMap.put(splittedKey, numberOfSplit);
            }
        }
        splittedKeyValueMap.put(key, valueSplitMap);
    }
  }
  
  private double calculateAvgFrequency(String key){
    HashMap<DataTypes,Long> valueFrequency = keyValueFrequencyMap.get(key);
    long sum = 0l;
    long count = 0l;
    for(Map.Entry<DataTypes,Long> entry : valueFrequency.entrySet()){
        sum = sum + entry.getValue();
        count++;
    }
    return ((double)sum/count);
  }

  private HashMap<String, List<Bucket<KeyValueFrequency>>> populateKeysToListOfBucketsMap()
      throws ClassNotFoundException, FileNotFoundException, KeeperException, InterruptedException,
      IOException, JAXBException {
    logger.info("Calculating keys to list of buckets map started");
    String[] keys = context.getShardingDimensions();
    int totalNoOfBuckets = bucketsCalculator.calculateNumberOfBucketsNeeded();
    logger.info("Total no. of buckets: {}", totalNoOfBuckets);
    HashMap<String, List<KeyValueFrequency>> keyToListOfKeyValueFrequency =
        getSortedKeyToListOfKeyValueFrequenciesMap();
    for (int i = 0; i < keys.length; i++) {
      HashMap<Object, Bucket<KeyValueFrequency>> valueToBucketMap = new HashMap<>();
      keyToValueToBucketMap.put(keys[i], valueToBucketMap);
      long frequencyOfAlreadyAddedValues = 0;
      int bucketCount = -1;
      HashMap<DataTypes, Long> frequencyPerValue = keyValueFrequencyMap.get(keys[i]);
      long idealAverageSizeOfOneBucket = getSizeOfOneBucket(frequencyPerValue, totalNoOfBuckets);
      logger.info("Ideal size of bucket for {}:{}",
          new Object[] {keys[i], idealAverageSizeOfOneBucket});
      Bucket<KeyValueFrequency> bucket = null;
      List<Bucket<KeyValueFrequency>> buckets = new ArrayList<>();
      long[] remainingCapacity = new long[totalNoOfBuckets];
      // buckets.add(bucket);
      List<KeyValueFrequency> sortedKeyValueFrequencies = keyToListOfKeyValueFrequency.get(keys[i]);
      int size = sortedKeyValueFrequencies.size();
      logger.info("size of {}:{}", new Object[] {keys[i], size});
      if (!sortedKeyValueFrequencies.isEmpty()) {
        for (KeyValueFrequency keyValueFrequency : sortedKeyValueFrequencies) {
            long numberOfSplits = 1;
            if(splittedKeyValueMap.get(keys[i]) != null 
                    && splittedKeyValueMap.get(keys[i]).get(keyValueFrequency.getKeyValue()) != null){
                numberOfSplits = splittedKeyValueMap.get(keys[i]).get(keyValueFrequency.getKeyValue());
            }
            long sizeOfCurrentBucket = idealAverageSizeOfOneBucket;
            long splittedFrequency = keyValueFrequency.getFrequency()/numberOfSplits;
            long remainderFrequency = keyValueFrequency.getFrequency()%numberOfSplits;
            
            long frequency = splittedFrequency;
            
            if(remainderFrequency > 0){
                frequency = splittedFrequency + 1;
                remainderFrequency--;
            }
            
            if(frequency > idealAverageSizeOfOneBucket){
                sizeOfCurrentBucket = frequency;
            }
            
            if(numberOfSplits > 1 && bucketCount < (totalNoOfBuckets - 1)){
                bucket = new Bucket<>(++bucketCount, sizeOfCurrentBucket);
                buckets.add(bucket);
                remainingCapacity[bucket.getId()] = sizeOfCurrentBucket;
                frequencyOfAlreadyAddedValues = 0;
            }
            
            for(int j = 0; j < numberOfSplits; j++){
                boolean flag = false;
                if(bucket == null){
                    flag = false;
                }else if(remainingCapacity[bucket.getId()] == sizeOfCurrentBucket){
                    flag = true;
                }else if((totalNoOfBuckets - (bucketCount + 1)) >= size){
                    flag = false;
                }else if(frequencyOfAlreadyAddedValues + frequency > sizeOfCurrentBucket){
                    for(int k = 0; k < totalNoOfBuckets; k++){
                        if(remainingCapacity[k] >= frequency){
                            bucket = buckets.get(k);
                            logger.info(
                                    "found a bucket {} which can accomodate the frequency, will not create new bucket",
                                    bucket.getId());
                            flag = true;
                            break;
                        }
                    }
                }else{
                  flag = true;
                }
                if(!flag){
                    if (bucketCount < (totalNoOfBuckets - 1)) {
                        bucket = new Bucket<>(++bucketCount, sizeOfCurrentBucket);
                        buckets.add(bucket);
                        remainingCapacity[bucket.getId()] = sizeOfCurrentBucket;
                        frequencyOfAlreadyAddedValues = 0;
                    }else{
                        logger.info(
                                  "Total Number of buckets are already created and will not create new bucket");
                        bucket = buckets.get(findTheLargestRemainingBucket(remainingCapacity));
                        logger.info(
                                  "remaining value is added to the bucket which has maximum remaning capacity:- {} ",
                                  bucket.getId());
                    }
                }
                frequencyOfAlreadyAddedValues = frequencyOfAlreadyAddedValues + frequency;
                remainingCapacity[bucket.getId()] = remainingCapacity[bucket.getId()] - frequency;
                bucket.add(keyValueFrequency);
                valueToBucketMap.put(DataTypesFactory.getNewInstance((DataTypes)keyValueFrequency.getKeyValue(),j), bucket);
                if(remainderFrequency > 0){
                    remainderFrequency--;
                }else{
                    frequency = splittedFrequency;
                }
            }
            size--;
        }
        this.keysToListOfBucketsMap.put(keys[i], buckets);
        logger.info("BucketCount is {} ", bucketCount + 1);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("keyValueFrequencyMap: " + MapUtils.getFormattedString(keyValueFrequencyMap));
    }
    logger.info("Calculating keys to list of buckets map finished");
    return this.keysToListOfBucketsMap;
  }


  private int findTheLargestRemainingBucket(long[] capacity) {
    int index = 0;
    if (capacity == null || capacity.length == 0) {
      logger.error("bucket array is empty or null");
    } else {

      long max = capacity[index];

      for (int i = 1; i < capacity.length; i++) {
        if (max < capacity[i]) {
          max = capacity[i];
          index = i;
        }
      }
    }
    return index;
  }

  /**
   * retrieves the sorted key list of frequency value map.
   * 
   * @return
   * @throws ClassNotFoundException
   * @throws FileNotFoundException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException
   * @throws JAXBException
   */
  public HashMap<String, List<KeyValueFrequency>> getSortedKeyToListOfKeyValueFrequenciesMap(){
    HashMap<String, List<KeyValueFrequency>> keyToListOfKeyValueFrequency = new HashMap<>();
    String[] keys = context.getShardingDimensions();
    for (String key : keys) {
      List<KeyValueFrequency> frequencies = new ArrayList<>();
      HashMap<DataTypes, Long> keyValueToFrequencyMap = keyValueFrequencyMap.get(key);
      for (Map.Entry<DataTypes, Long> keyValueEntry : keyValueToFrequencyMap.entrySet()) {
        frequencies.add(new KeyValueFrequency(keyValueEntry.getKey(), keyValueEntry.getValue()));
      }
      Collections.sort(frequencies);
      keyToListOfKeyValueFrequency.put(key, frequencies);
    }
    return keyToListOfKeyValueFrequency;
  }

  private long getSizeOfOneBucket(HashMap<DataTypes, Long> frequencyPerValue, int noOfBuckets) {
    long sizeOfOneBucket = 0;
    long totalofAllKeyValueFrequencies = 0;
    for (Map.Entry<DataTypes, Long>  mutableCharArrayStringEntry : frequencyPerValue.entrySet()) {
      totalofAllKeyValueFrequencies =
          totalofAllKeyValueFrequencies + mutableCharArrayStringEntry.getValue();
      sizeOfOneBucket = totalofAllKeyValueFrequencies / (noOfBuckets);
    }
    return sizeOfOneBucket;
  }

  private void shardSingleKey(String keyName) throws NodeOverflowException {
    List<Bucket<KeyValueFrequency>> buckets = keysToListOfBucketsMap.get(keyName);
    HashMap<Bucket<KeyValueFrequency>, Node> bucketToNodeNumber = new HashMap<>();
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
      HashMap<String, Bucket<KeyValueFrequency>> wouldBeMap = new HashMap<>();
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
    for (HashMap.Entry<BucketCombination, Long> entry : bucketCombinationFrequencyMap.entrySet()) {
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
      Set<Node> nodesForBucketCombination = new LinkedHashSet<>();
      for (int i = 0; i < keys.length; i++) {
        Node n =
            bucketToNodeNumberMap.get(keys[i]).get(source.getBucketsCombination().get(keys[i]));
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
        HashMap<String, Bucket<KeyValueFrequency>> nextSourceMap = new HashMap<>();
        nextSourceMap.putAll(source.getBucketsCombination());
        BucketCombination nextSource = new BucketCombination(nextSourceMap);
        nextSource.getBucketsCombination().put(keyName, bucket);
        makeShardingTable(nextSource, restOfKeyNames);
      }
    }
  }

}
