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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

/**
 * 
 *{@code BucketsCalculator} used for calculating the size of Buckets.
 */
public final class BucketsCalculator {

  private static final int NO_OF_BYTES_PER_KEY = 4;

  private static final int NO_OF_BYTES_STORING_A_BUCKET_OBJECT_IN_SHARD_TABLE_TAKES = 4;

  private HashMap<String, HashMap<String, List<Bucket<KeyValueFrequency>>>> keyToValueToBucketMap = null;

  private HashMap<String, List<Bucket<KeyValueFrequency>>> keyToBucketNumbersCollectionMap =
      new HashMap<String, List<Bucket<KeyValueFrequency>>>();
 private ShardingApplicationContext context;
 
 /**
  * creates an instance.
  * @param context
  */
  public BucketsCalculator(ShardingApplicationContext context) {
 this.context = context;
  }

  /**
   * create an instance of BucketsCalculator.
   * @param keyToValueToBucketMap
   * @param context
   */
  public BucketsCalculator(
          HashMap<String, HashMap<String, List<Bucket<KeyValueFrequency>>>> keyToValueToBucketMap,ShardingApplicationContext context) {
    this(context);
    this.keyToValueToBucketMap = keyToValueToBucketMap;
    if (keyToValueToBucketMap != null) {
      for (Map.Entry<String, HashMap<String, List<Bucket<KeyValueFrequency>>>> keyValueEntry : keyToValueToBucketMap.entrySet()) {
        Set<Bucket<KeyValueFrequency>> totalBuckets = new HashSet<>();
          for(Map.Entry<String, List<Bucket<KeyValueFrequency>>> valueListEntry : keyValueEntry.getValue().entrySet()){
              totalBuckets.addAll(valueListEntry.getValue());
          }
        keyToBucketNumbersCollectionMap.put(keyValueEntry.getKey(), new ArrayList<>(totalBuckets));
      }
    }
  }

  /**
   * calculates number of buckets needed.
   * @return
   * @throws ClassNotFoundException
   * @throws FileNotFoundException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException
   * @throws JAXBException
   */
  public  int calculateNumberOfBucketsNeeded() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    double MAX_NO_OF_FILE_SIZE = Double.valueOf(
        context.getShardingServerConfig().getMaximumShardFileSizeInBytes());
    int noOfKeys = context.getShardingDimensions().length;
    long approximateMemoryPerBucketStoredInShardTable =
        (NO_OF_BYTES_PER_KEY * noOfKeys) + NO_OF_BYTES_STORING_A_BUCKET_OBJECT_IN_SHARD_TABLE_TAKES;
    Double noOfBucketsNeeded = Math
        .pow(MAX_NO_OF_FILE_SIZE / approximateMemoryPerBucketStoredInShardTable, 1.0 / noOfKeys);
    int numberOfBucketsNeeded = (int) Math.ceil(noOfBucketsNeeded);
    Object maxNoOfBuckets =
        context.getShardingServerConfig().getMaximumNoOfShardBucketsSize();
    if (maxNoOfBuckets != null) {
      int maximumNoOfBucketsAllowed = Integer.parseInt(maxNoOfBuckets.toString());
      if (numberOfBucketsNeeded > maximumNoOfBucketsAllowed) {
        numberOfBucketsNeeded = maximumNoOfBucketsAllowed;
      }
    }
    return numberOfBucketsNeeded;
  }

  /**
   * Retrieves the Bucket which KeyValueFrequency.
   * @param key
   * @param value
   * @return
   */
  public Bucket<KeyValueFrequency> getBucketNumberForValue(String key, final String value, int idx) {
      Bucket<KeyValueFrequency> bucket = null;
      if (keyToValueToBucketMap != null) {
          Map<String, List<Bucket<KeyValueFrequency>>> valueToBucketMap = keyToValueToBucketMap.get(key);
          Bucket<KeyValueFrequency> valueBucket = null;
          if (valueToBucketMap != null) {
              List<Bucket<KeyValueFrequency>> valueBucketList = valueToBucketMap.get(value);
              if (valueBucketList != null) {
                  valueBucket = valueBucketList.get(idx);
              }
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
