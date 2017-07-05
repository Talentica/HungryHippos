/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.spark.Partition;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.util.NodeSelector;

import scala.Tuple2;

/**
 * Created by rajkishoreh on 30/12/16.
 */
public class HHRDDInfoImpl implements HHRDDInfo {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = -7374519945508249959L;

  /** The bucket combination to node number map. *//*
  public Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap;*/

  /** The bucket to node number map. */
  private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;

  /** The key order. */
  private String[] keyOrder;

  /** The no of dimensions. */
  private int noOfDimensions;

  /** The file name to node ids cache. */
  private Map<String, int[]> fileNameToNodeIdsCache;

  /** The nod id to ip. */
  private Map<Integer, SerializedNode> nodIdToIp;

  /** The file name to size whole map. */
  private Map<String, Long> fileNameToSizeWholeMap;

  /** The file to node id. */
  private Map<String, Tuple2<String, int[]>> fileToNodeId;

  /** The key to bucket to file list. */
  private Map<String, Map<Integer, List<String>>> keyToBucketToFileList;

  /** The sharding indexes. */
  private int[] shardingIndexes;

  /** The field data desc. */
  private FieldTypeArrayDataDescription fieldDataDesc;

  /** The directory location. */
  private String directoryLocation;

  /** The hh file size. */
  private long hhFileSize;
  
  private NodeSelector nodeSelector;


  /**
   * Instantiates a new HHRDD info impl.
   *
   * @param bucketToNodeNumberMap the bucket to node number map
   * @param fileNameToSizeWholeMap the file name to size whole map
   * @param keyOrder the key order
   * @param nodIdToIp the nod id to ip
   * @param shardingIndexes the sharding indexes
   * @param fieldDataDesc the field data desc
   * @param directoryLocation the directory location
   */
  public HHRDDInfoImpl(
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      Map<String, Long> fileNameToSizeWholeMap, String[] keyOrder,
      Map<Integer, SerializedNode> nodIdToIp, int[] shardingIndexes,
      FieldTypeArrayDataDescription fieldDataDesc, String directoryLocation) {
    //this.bucketCombinationToNodeNumberMap = bucketCombinationToNodeNumberMap;
    nodeSelector = new NodeSelector();
    this.bucketToNodeNumberMap = bucketToNodeNumberMap;
    this.keyOrder = keyOrder;
    this.nodIdToIp = nodIdToIp;
    this.fileNameToNodeIdsCache = new HashMap<>();
    this.noOfDimensions = keyOrder.length;
    this.fileNameToSizeWholeMap = fileNameToSizeWholeMap;
    this.fileToNodeId = new HashMap<>();
    this.keyToBucketToFileList = new HashMap<>();
    this.shardingIndexes = shardingIndexes;
    this.fieldDataDesc = fieldDataDesc;
    this.directoryLocation = directoryLocation;
    this.hhFileSize = 0;
    initializeKeyToBucketToFileList();
    calculateBucketToFileMap("", 0);
  }

  /**
   * Gets the bucket combination to node number map.
   *
   * @return the bucket combination to node number map
   */
  /*public Map<BucketCombination, Set<Node>> getBucketCombinationToNodeNumberMap() {
    return bucketCombinationToNodeNumberMap;
  }*/

  /*
   * (non-Javadoc)
   * 
   * @see com.talentica.hungryHippos.rdd.HHRDDInfo#getBucketToNodeNumberMap()
   */
  @Override
  public HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    return bucketToNodeNumberMap;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.talentica.hungryHippos.rdd.HHRDDInfo#getKeyOrder()
   */
  @Override
  public String[] getKeyOrder() {
    return keyOrder;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.talentica.hungryHippos.rdd.HHRDDInfo#getShardingIndexes()
   */
  @Override
  public int[] getShardingIndexes() {
    return shardingIndexes;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.talentica.hungryHippos.rdd.HHRDDInfo#getFieldDataDesc()
   */
  @Override
  public FieldTypeArrayDataDescription getFieldDataDesc() {
    return fieldDataDesc;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.talentica.hungryHippos.rdd.HHRDDInfo#getPartitions(int, int, java.util.List, int,
   * java.util.List, java.lang.String)
   */
  @Override
  public Partition[] getPartitions(int id, int noOfExecutors, List<Integer> jobShardingDimensions,
      int jobPrimaryDimensionIdx, List<String> jobShardingDimensionsKey,
      String primaryDimensionKey) {
    int noOfShardingDimensions = keyOrder.length;
    int noOfEstimatedPartitions = 1;
    int[] jobShardingDimensionsArray = new int[jobShardingDimensions.size()];
    int i = 0;
    noOfEstimatedPartitions = getNoOfEstimatedPartitions(jobShardingDimensions,
        jobShardingDimensionsKey, noOfEstimatedPartitions, jobShardingDimensionsArray, i);
    System.out.println();
    int[][] combinationArray = new int[noOfEstimatedPartitions][];
    populateCombination(combinationArray, null, 0, jobShardingDimensionsArray, 0);
    Partition[] partitions = null;
    if (noOfEstimatedPartitions <= noOfExecutors) {
      partitions = getPartitionsWhenEstimatedPartitionsIsLess(id, jobPrimaryDimensionIdx,
          primaryDimensionKey, noOfShardingDimensions, noOfEstimatedPartitions,
          jobShardingDimensionsArray, combinationArray);
    } else {
      partitions = getPartitionsWhenEstimatedPartitionsIsMore(id, noOfShardingDimensions,
          noOfEstimatedPartitions, jobShardingDimensionsArray, combinationArray);
    }
    return partitions;
  }

  /**
   * Gets the no of estimated partitions.
   *
   * @param jobShardingDimensions the job sharding dimensions
   * @param jobShardingDimensionsKey the job sharding dimensions key
   * @param noOfEstimatedPartitions the no of estimated partitions
   * @param jobShardingDimensionsArray the job sharding dimensions array
   * @param i the i
   * @return the no of estimated partitions
   */
  private int getNoOfEstimatedPartitions(List<Integer> jobShardingDimensions,
      List<String> jobShardingDimensionsKey, int noOfEstimatedPartitions,
      int[] jobShardingDimensionsArray, int i) {
    for (String shardingDimensionKey : jobShardingDimensionsKey) {
      int bucketSize = bucketToNodeNumberMap.get(shardingDimensionKey).size();
      noOfEstimatedPartitions = noOfEstimatedPartitions * bucketSize;
      jobShardingDimensionsArray[i] = jobShardingDimensions.get(i);
      System.out.print(jobShardingDimensionsArray[i]);
      i++;

    }
    return noOfEstimatedPartitions;
  }

  /**
   * Populate combination.
   *
   * @param combinationArray the combination array
   * @param combination the combination
   * @param index the index
   * @param jobShardingDimensions the job sharding dimensions
   * @param i the i
   * @return the int
   */
  private int populateCombination(int[][] combinationArray, String combination, int index,
      int[] jobShardingDimensions, int i) {
    if (i == jobShardingDimensions.length) {
      String[] strings = combination.split("-");
      int[] intCombination = new int[strings.length];
      for (int j = 0; j < strings.length; j++) {
        intCombination[j] = Integer.parseInt(strings[j]);
      }
      combinationArray[index] = intCombination;
      index++;
      return index;
    }
    for (int j = 0; j < bucketToNodeNumberMap.get(keyOrder[jobShardingDimensions[i]]).size(); j++) {
      String newCombination;
      if (i != 0) {
        newCombination = combination + "-" + j;
      } else {
        newCombination = j + "";
      }
      index = populateCombination(combinationArray, newCombination, index, jobShardingDimensions,
          i + 1);
    }
    return index;
  }

  /**
   * Gets the partitions when estimated partitions is less.
   *
   * @param id the id
   * @param jobPrimaryDimensionIdx the job primary dimension idx
   * @param primaryDimensionKey the primary dimension key
   * @param noOfShardingDimensions the no of sharding dimensions
   * @param noOfEstimatedPartitions the no of estimated partitions
   * @param jobShardingDimensionsArray the job sharding dimensions array
   * @param combinationArray the combination array
   * @return the partitions when estimated partitions is less
   */
  private Partition[] getPartitionsWhenEstimatedPartitionsIsLess(int id, int jobPrimaryDimensionIdx,
      String primaryDimensionKey, int noOfShardingDimensions, int noOfEstimatedPartitions,
      int[] jobShardingDimensionsArray, int[][] combinationArray) {
    Partition[] partitions;
    partitions = new HHRDDPartition[noOfEstimatedPartitions];
    for (int index = 0; index < noOfEstimatedPartitions; index++) {
      List<Tuple2<String, int[]>> files = new ArrayList<>();
      listFile(files, "", 0, noOfShardingDimensions, jobShardingDimensionsArray,
          combinationArray[index]);
      System.out.println();
      int preferredNodeId = bucketToNodeNumberMap.get(primaryDimensionKey)
          .get(new Bucket<>(combinationArray[index][jobPrimaryDimensionIdx])).getNodeId();
      List<String> preferredHosts = new ArrayList<>();
      preferredHosts.add(nodIdToIp.get(preferredNodeId).getIp());
      partitions[index] = new HHRDDPartition(id, index, new File(this.directoryLocation).getPath(),
          this.fieldDataDesc, preferredHosts, files, nodIdToIp);
    }
    return partitions;
  }

  /**
   * Gets the partitions when estimated partitions is more.
   *
   * @param id the id
   * @param noOfShardingDimensions the no of sharding dimensions
   * @param noOfEstimatedPartitions the no of estimated partitions
   * @param jobShardingDimensionsArray the job sharding dimensions array
   * @param combinationArray the combination array
   * @return the partitions when estimated partitions is more
   */
  private Partition[] getPartitionsWhenEstimatedPartitionsIsMore(int id, int noOfShardingDimensions,
      int noOfEstimatedPartitions, int[] jobShardingDimensionsArray, int[][] combinationArray) {
    Partition[] partitions;
    long idealPartitionFileSize = 128 * 1024 * 1024;
    PriorityQueue<PartitionBucket> partitionBuckets = new PriorityQueue<>();
    int fileCount =
        preparePartitionBucketsAndGetFileCount(noOfShardingDimensions, noOfEstimatedPartitions,
            jobShardingDimensionsArray, combinationArray, idealPartitionFileSize, partitionBuckets);
    int partitionIdx = 0;
    Iterator<PartitionBucket> partitionBucketIterator = partitionBuckets.iterator();
    List<Partition> listOfPartitions = new ArrayList<>();
    while (partitionBucketIterator.hasNext()) {
      partitionIdx = addPartitionToListAndGetPartitionIdx(id, partitionIdx, partitionBucketIterator,
          listOfPartitions);
    }
    System.out.println("file count : " + fileCount);
    System.out.println("PartitionSize : " + listOfPartitions.size());
    partitions = new Partition[listOfPartitions.size()];
    for (int j = 0; j < partitions.length; j++) {
      partitions[j] = listOfPartitions.get(j);
    }
    return partitions;
  }

  /**
   * Prepare partition buckets and get file count.
   *
   * @param noOfShardingDimensions the no of sharding dimensions
   * @param noOfEstimatedPartitions the no of estimated partitions
   * @param jobShardingDimensionsArray the job sharding dimensions array
   * @param combinationArray the combination array
   * @param idealPartitionFileSize the ideal partition file size
   * @param partitionBuckets the partition buckets
   * @return the int
   */
  private int preparePartitionBucketsAndGetFileCount(int noOfShardingDimensions,
      int noOfEstimatedPartitions, int[] jobShardingDimensionsArray, int[][] combinationArray,
      long idealPartitionFileSize, PriorityQueue<PartitionBucket> partitionBuckets) {
    PartitionBucket partitionBucket = new PartitionBucket(0);
    partitionBuckets.offer(partitionBucket);
    int fileCount = 0;
    for (int index = 0; index < noOfEstimatedPartitions; index++) {
      partitionBucket = partitionBuckets.poll();
      List<Tuple2<String, int[]>> files = new ArrayList<>();
      listFile(files, "", 0, noOfShardingDimensions, jobShardingDimensionsArray,
          combinationArray[index]);
      long listFileSize = 0;
      for (int j = 0; j < files.size(); j++) {
        listFileSize += fileNameToSizeWholeMap.get(files.get(j)._1);
      }
      if (partitionBucket.getSize() + listFileSize > idealPartitionFileSize
          && partitionBucket.getSize() != 0) {
        partitionBuckets.offer(partitionBucket);
        partitionBucket = new PartitionBucket(0);
      }
      for (int j = 0; j < files.size(); j++) {
        String fileName = files.get(j)._1;
        partitionBucket.addFile(fileToNodeId.get(fileName), fileNameToSizeWholeMap.get(fileName));
        fileCount++;
      }
      partitionBuckets.offer(partitionBucket);
    }
    return fileCount;
  }

  /**
   * Adds the partition to list and get partition idx.
   *
   * @param id the id
   * @param partitionIdx the partition idx
   * @param partitionBucketIterator the partition bucket iterator
   * @param listOfPartitions the list of partitions
   * @return the int
   */
  private int addPartitionToListAndGetPartitionIdx(int id, int partitionIdx,
      Iterator<PartitionBucket> partitionBucketIterator, List<Partition> listOfPartitions) {
    PartitionBucket partitionBucket1 = partitionBucketIterator.next();
    PriorityQueue<NodeBucket> nodeBuckets = new PriorityQueue<>();
    for (Map.Entry<Integer, NodeBucket> nodeBucketEntry : partitionBucket1.getNodeBucketMap()
        .entrySet()) {
      nodeBuckets.offer(nodeBucketEntry.getValue());
    }
    int maxNoOfPreferredNodes = 3;// No of Preferred Nodes
    int remNoOfPreferredNodes = maxNoOfPreferredNodes;
    NodeBucket nodeBucket;
    List<String> preferredIpList = new ArrayList<>();
    while ((nodeBucket = nodeBuckets.poll()) != null && remNoOfPreferredNodes > 0) {
      preferredIpList.add(nodIdToIp.get(nodeBucket.getId()).getIp());
      remNoOfPreferredNodes--;
    }
    List<Tuple2<String, int[]>> files = partitionBucket1.getFiles();
    if (!files.isEmpty()) {
      Partition partition =
          new HHRDDPartition(id, partitionIdx, new File(this.directoryLocation).getPath(),
              this.fieldDataDesc, preferredIpList, files, nodIdToIp);
      partitionIdx++;
      listOfPartitions.add(partition);
    }
    return partitionIdx;
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.talentica.hungryHippos.rdd.HHRDDInfo#getOptimizedPartitions(int, int, java.util.List,
   * int, java.util.List, java.lang.String)
   */
  @Override
  public Partition[] getOptimizedPartitions(int id, int noOfExecutors,
      List<Integer> jobShardingDimensions, int jobPrimaryDimensionIdx,
      List<String> jobShardingDimensionsKey, String primaryDimensionKey) {
    int totalCombination = fileNameToSizeWholeMap.size();
    System.out.println("jobShardingDimensions " + jobShardingDimensions);
    System.out.println("jobShardingDimensionsKey " + jobShardingDimensionsKey);
    Partition[] partitions;
    if (noOfExecutors < totalCombination) {
      partitions = getOptimizedPartitionsWhenTotalCombinationIsMore(id, primaryDimensionKey);
    } else {
      partitions = getOptimizedPartitionsWhenTotalCombinationIsLess(id, jobShardingDimensions,
          jobPrimaryDimensionIdx, totalCombination);
    }
    return partitions;
  }

  /**
   * Gets the optimized partitions when total combination is more.
   *
   * @param id the id
   * @param primaryDimensionKey the primary dimension key
   * @return the optimized partitions when total combination is more
   */
  private Partition[] getOptimizedPartitionsWhenTotalCombinationIsMore(int id,
      String primaryDimensionKey) {
    Partition[] partitions;
    long idealPartitionFileSize = 128 * 1024 * 1024;// 128MB partition size
    List<Partition> listOfPartitions = new ArrayList<>();
    int partitionIdx = 0;
    int fileCount = 0;
    Set<String> fileNamesSet = new HashSet<>();
    PriorityQueue<PartitionBucket> partitionBuckets = new PriorityQueue<>();
    fileCount = preparePartitionsAndGetFileCount(primaryDimensionKey, idealPartitionFileSize,
        fileCount, fileNamesSet, partitionBuckets);
    System.out.println("No of unique files : " + fileNamesSet.size());
    Iterator<PartitionBucket> partitionBucketIterator = partitionBuckets.iterator();
    while (partitionBucketIterator.hasNext()) {
      partitionIdx = addPartitionToListAndGetPartitionIdx(id, partitionIdx, partitionBucketIterator,
          listOfPartitions);
    }
    System.out.println("file count : " + fileCount);
    System.out.println("PartitionSize : " + listOfPartitions.size());
    partitions = new Partition[listOfPartitions.size()];
    for (int j = 0; j < partitions.length; j++) {
      partitions[j] = listOfPartitions.get(j);
    }
    return partitions;
  }

  /**
   * Prepare partitions and get file count.
   *
   * @param primaryDimensionKey the primary dimension key
   * @param idealPartitionFileSize the ideal partition file size
   * @param fileCount the file count
   * @param fileNamesSet the file names set
   * @param partitionBuckets the partition buckets
   * @return the int
   */
  private int preparePartitionsAndGetFileCount(String primaryDimensionKey,
      long idealPartitionFileSize, int fileCount, Set<String> fileNamesSet,
      PriorityQueue<PartitionBucket> partitionBuckets) {
    PartitionBucket partitionBucket = new PartitionBucket(0);
    partitionBuckets.offer(partitionBucket);
    for (Map.Entry<Integer, List<String>> entry : keyToBucketToFileList.get(primaryDimensionKey)
        .entrySet()) {
      for (String fileName : entry.getValue()) {
        long fileSize = fileNameToSizeWholeMap.get(fileName);
        partitionBucket = partitionBuckets.poll();
        if (partitionBucket.getSize() + fileSize > idealPartitionFileSize
            && partitionBucket.getSize() != 0) {
          partitionBuckets.offer(partitionBucket);
          partitionBucket = new PartitionBucket(0);
        }
        partitionBucket.addFile(fileToNodeId.get(fileName), fileSize);
        partitionBuckets.offer(partitionBucket);
        fileNamesSet.add(fileName);
        fileCount++;
      }
    }
    return fileCount;
  }

  /**
   * Gets the optimized partitions when total combination is less.
   *
   * @param id the id
   * @param jobShardingDimensions the job sharding dimensions
   * @param jobPrimaryDimensionIdx the job primary dimension idx
   * @param totalCombination the total combination
   * @return the optimized partitions when total combination is less
   */
  private Partition[] getOptimizedPartitionsWhenTotalCombinationIsLess(int id,
      List<Integer> jobShardingDimensions, int jobPrimaryDimensionIdx, int totalCombination) {
    Partition[] partitions;
    partitions = new Partition[totalCombination];
    int index = 0;
    for (Map.Entry<String, Tuple2<String, int[]>> fileEntry : fileToNodeId.entrySet()) {
      List<Tuple2<String, int[]>> files = new ArrayList<>();
      files.add(fileEntry.getValue());
      int preferredNodeId =
          fileEntry.getValue()._2[jobShardingDimensions.get(jobPrimaryDimensionIdx)];
      List<String> preferredHosts = new ArrayList<>();
      preferredHosts.add(nodIdToIp.get(preferredNodeId).getIp());
      partitions[index] = new HHRDDPartition(id, index, new File(this.directoryLocation).getPath(),
          this.fieldDataDesc, preferredHosts, files, nodIdToIp);
      index++;
    }
    return partitions;
  }

  /**
   * List file.
   *
   * @param files the files
   * @param fileName the file name
   * @param dim the dim
   * @param noOfShardingDimensions the no of sharding dimensions
   * @param jobShardingDimensionsArray the job sharding dimensions array
   * @param jobDimensionValues the job dimension values
   */
  private void listFile(List<Tuple2<String, int[]>> files, String fileName, int dim,
      int noOfShardingDimensions, int[] jobShardingDimensionsArray, int[] jobDimensionValues) {
    if (dim == noOfShardingDimensions) {
      Tuple2<String, int[]> tuple2 = fileToNodeId.get(fileName);
      files.add(tuple2);
      return;
    }
    boolean isJobShardingDimension = false;
    int jobDimIdx = 0;
    for (int i = 0; i < jobShardingDimensionsArray.length; i++) {
      if (dim == jobShardingDimensionsArray[i]) {
        isJobShardingDimension = true;
        break;
      }
      jobDimIdx++;
    }
    if (isJobShardingDimension) {
      if (dim == 0) {
        listFile(files, jobDimensionValues[jobDimIdx] + fileName, dim + 1, noOfShardingDimensions,
            jobShardingDimensionsArray, jobDimensionValues);
      } else {
        listFile(files, fileName + "_" + jobDimensionValues[jobDimIdx], dim + 1,
            noOfShardingDimensions, jobShardingDimensionsArray, jobDimensionValues);
      }
    } else {
      for (int i = 0; i < bucketToNodeNumberMap.get(keyOrder[dim]).size(); i++) {
        if (dim == 0) {
          listFile(files, i + fileName, dim + 1, noOfShardingDimensions, jobShardingDimensionsArray,
              jobDimensionValues);
        } else {
          listFile(files, fileName + "_" + i, dim + 1, noOfShardingDimensions,
              jobShardingDimensionsArray, jobDimensionValues);
        }
      }
    }
  }

  /**
   * Gets the file location node ids.
   *
   * @param fileName the file name
   * @return the file location node ids
   */
  private int[] getFileLocationNodeIds(String fileName) {
    int[] nodeIds = fileNameToNodeIdsCache.get(fileName);
    if (nodeIds == null) {
      nodeIds = new int[keyOrder.length];
      String[] buckets = fileName.split("_");
      Map<String, Bucket<KeyValueFrequency>> keyValueCombination = new HashMap<>();
      for (int i = 0; i < keyOrder.length; i++) {
        keyValueCombination.put(keyOrder[i], new Bucket<>(Integer.parseInt(buckets[i])));
      }
      BucketCombination bucketCombination = new BucketCombination(keyValueCombination);
      Set<Integer> ids =
          nodeSelector.selectNodeIds(bucketCombination, bucketToNodeNumberMap, keyOrder);
      int i = 0;
      for (int nodeId : ids) {
        nodeIds[i] = nodeId;
        i++;
      }

      fileNameToNodeIdsCache.put(fileName, nodeIds);
    }
    return nodeIds;
  }


  /**
   * Initialize key to bucket to file list.
   */
  private void initializeKeyToBucketToFileList() {
    for (int dim = 0; dim < keyOrder.length; dim++) {
      Map<Integer, List<String>> bucketToFileList = new HashMap<>();
      keyToBucketToFileList.put(keyOrder[dim]+dim, bucketToFileList);
      for (Bucket<KeyValueFrequency> bucket : bucketToNodeNumberMap.get(keyOrder[dim]).keySet()) {
        List<String> fileList = new ArrayList<>();
        bucketToFileList.put(bucket.getId(), fileList);
      }
    }
  }

  /**
   * Calculate bucket to file map.
   *
   * @param fileName the file name
   * @param dim the dim
   */
  private void calculateBucketToFileMap(String fileName, int dim) {
    if (dim == noOfDimensions) {
      Tuple2<String, int[]> tuple2 = new Tuple2<>(fileName, getFileLocationNodeIds(fileName));
      fileToNodeId.put(fileName, tuple2);
      hhFileSize += fileNameToSizeWholeMap.get(fileName);
      String[] buckets = fileName.split("_");
      for (int i = 0; i < noOfDimensions; i++) {
        keyToBucketToFileList.get(keyOrder[i]+i).get(Integer.parseInt(buckets[i])).add(fileName);
      }
      return;
    }
    for (Bucket<KeyValueFrequency> bucket : bucketToNodeNumberMap.get(keyOrder[dim]).keySet()) {
      if (dim == 0) {
        calculateBucketToFileMap(bucket.getId() + fileName, dim + 1);
      } else {
        calculateBucketToFileMap(fileName + "_" + bucket.getId(), dim + 1);
      }
    }
  }

  /**
   * The Class PartitionBucket.
   */
  static class PartitionBucket implements Comparable<PartitionBucket> {

    /** The size. */
    long size;

    /** The files. */
    List<Tuple2<String, int[]>> files;

    /** The node bucket map. */
    Map<Integer, NodeBucket> nodeBucketMap;

    /**
     * Instantiates a new partition bucket.
     *
     * @param size the size
     */
    public PartitionBucket(long size) {
      this.size = size;
      this.files = new ArrayList<>();
      this.nodeBucketMap = new HashMap<>();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(PartitionBucket o) {
      return Long.compare(size, o.size);
    }

    /**
     * Adds the file.
     *
     * @param file the file
     * @param size the size
     */
    private void addFile(Tuple2<String, int[]> file, long size) {
      files.add(file);
      int[] nodeIds = file._2;
      for (int i = 0; i < nodeIds.length; i++) {
        int nodeId = nodeIds[i];
        NodeBucket nodeBucket = nodeBucketMap.get(nodeId);
        if (nodeBucket == null) {
          nodeBucket = new NodeBucket(nodeId, 0);
          nodeBucketMap.put(nodeId, nodeBucket);
        }
        nodeBucket.addSize(size);
      }
      this.size += size;
    }

    /**
     * Gets the files.
     *
     * @return the files
     */
    public List<Tuple2<String, int[]>> getFiles() {
      return files;
    }

    /**
     * Gets the size.
     *
     * @return the size
     */
    public long getSize() {
      return size;
    }

    /**
     * Gets the node bucket map.
     *
     * @return the node bucket map
     */
    public Map<Integer, NodeBucket> getNodeBucketMap() {
      return nodeBucketMap;
    }
  }

  /**
   * The Class NodeBucket.
   */
  static class NodeBucket implements Comparable<NodeBucket> {

    /** The id. */
    int id;

    /** The size. */
    long size;

    /**
     * Instantiates a new node bucket.
     *
     * @param id the id
     * @param size the size
     */
    public NodeBucket(int id, long size) {
      this.id = id;
      this.size = size;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(NodeBucket o) {
      return Long.compare(o.size, size);
    }

    /**
     * Adds the size.
     *
     * @param size the size
     */
    private void addSize(long size) {
      this.size += size;
    }

    /**
     * Gets the size.
     *
     * @return the size
     */
    public long getSize() {
      return size;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public int getId() {
      return id;
    }
  }
}
