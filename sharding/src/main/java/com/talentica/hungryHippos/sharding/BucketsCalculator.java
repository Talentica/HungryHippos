package com.talentica.hungryHippos.sharding;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

public final class BucketsCalculator {

  private static final int NO_OF_BYTES_PER_KEY = 4;

  private static final int NO_OF_BYTES_STORING_A_BUCKET_OBJECT_IN_SHARD_TABLE_TAKES = 4;

  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;

  private Map<String, List<Bucket<KeyValueFrequency>>> keyToBucketNumbersCollectionMap =
      new HashMap<String, List<Bucket<KeyValueFrequency>>>();

  public BucketsCalculator() {

  }

  public BucketsCalculator(
      Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap) {
    this.keyToValueToBucketMap = keyToValueToBucketMap;
    if (keyToValueToBucketMap != null) {
      for (String key : keyToValueToBucketMap.keySet()) {
        List<Bucket<KeyValueFrequency>> totalBuckets = new ArrayList<>();
        Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = keyToValueToBucketMap.get(key);
        for (Object keyValue : valueToBucketMap.keySet()) {
          totalBuckets.add(valueToBucketMap.get(keyValue));
        }
        keyToBucketNumbersCollectionMap.put(key, new ArrayList<>(new HashSet<>(totalBuckets)));
      }
    }
  }

  public static int calculateNumberOfBucketsNeeded(String path) throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    double MAX_NO_OF_FILE_SIZE = Double.valueOf(
        ShardingApplicationContext.getShardingServerConfig(path).getMaximumShardFileSizeInBytes());
    int noOfKeys = ShardingApplicationContext.getShardingDimensions(path).length;
    long approximateMemoryPerBucketStoredInShardTable =
        (NO_OF_BYTES_PER_KEY * noOfKeys) + NO_OF_BYTES_STORING_A_BUCKET_OBJECT_IN_SHARD_TABLE_TAKES;
    Double noOfBucketsNeeded = Math
        .pow(MAX_NO_OF_FILE_SIZE / approximateMemoryPerBucketStoredInShardTable, 1.0 / noOfKeys);
    int numberOfBucketsNeeded = (int) Math.ceil(noOfBucketsNeeded);
    Object maxNoOfBuckets =
        ShardingApplicationContext.getShardingServerConfig(path).getMaximumNoOfShardBucketsSize();
    if (maxNoOfBuckets != null) {
      int maximumNoOfBucketsAllowed = Integer.parseInt(maxNoOfBuckets.toString());
      if (numberOfBucketsNeeded > maximumNoOfBucketsAllowed) {
        numberOfBucketsNeeded = maximumNoOfBucketsAllowed;
      }
    }
    return numberOfBucketsNeeded;
  }

  public Bucket<KeyValueFrequency> getBucketNumberForValue(String key, Object value) {
    Bucket<KeyValueFrequency> bucket = null;
    if (keyToValueToBucketMap != null) {
      Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = keyToValueToBucketMap.get(key);
      if (valueToBucketMap != null) {
        Bucket<KeyValueFrequency> valueBucket = valueToBucketMap.get(value);
        if (valueBucket != null) {
          bucket = valueBucket;
        } else {
          bucket = calculateBucketNumberForNewValue(key, value);
        }
      }
    }
    if (bucket == null) {
      bucket = new Bucket<>(0);
    }
    return bucket;
  }

  private Bucket<KeyValueFrequency> calculateBucketNumberForNewValue(String key, Object value) {
    Bucket<KeyValueFrequency> bucket = null;
    if (keyToBucketNumbersCollectionMap != null
        && keyToBucketNumbersCollectionMap.get(key) != null) {
      List<Bucket<KeyValueFrequency>> bucketsForKey = keyToBucketNumbersCollectionMap.get(key);
      int totalNumberOfBuckets = bucketsForKey.size();
      if (totalNumberOfBuckets > 1) {
        int hashCode = calculateHashCode(value);
        int bucketNumber = hashCode % totalNumberOfBuckets + 1;
        Bucket<KeyValueFrequency> bucketToBeAllotted = new Bucket<KeyValueFrequency>(bucketNumber);
        if (bucketsForKey.contains(bucketToBeAllotted)) {
          bucket = bucketsForKey.get(bucketsForKey.indexOf(bucketToBeAllotted));
        }
      } else if (totalNumberOfBuckets == 1) {
        bucket = bucketsForKey.iterator().next();
      }
    }
    return bucket;
  }

  private static int calculateHashCode(Object value) {
    int hashCode = value.hashCode();
    if (hashCode == Integer.MIN_VALUE) {
      hashCode = Integer.MAX_VALUE;
    } else {
      hashCode = Math.abs(hashCode);
    }
    return hashCode;
  }
}
