package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.spark.Partition;

import com.google.common.io.Files;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;

import scala.Tuple2;

/**
 * Created by rajkishoreh on 30/12/16.
 */
public class HHRDDInfo implements Serializable {
  private static final long serialVersionUID = -2950451610425126517L;
  public Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap;
  private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;
  private String[] keyOrder;
  private int noOfDimensions;
  private Map<String, int[]> fileNameToNodeIdsCache;
  private Map<Integer, SerializedNode> nodeInfo;
  private Map<String, Long> fileNameToSizeWholeMap;
  private Map<String, Tuple2<String, int[]>> fileToNodeId;
  private Map<String, Map<Integer, List<String>>> keyToBucketToFileList;
  private int[] shardingIndexes;
  private FieldTypeArrayDataDescription fieldDataDesc;
  private String directoryLocation;
  private long hhFileSize;
  private String dataDirectoryLocation;
  private static File tmpDirectory = null;


  public HHRDDInfo(Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      Map<String, Long> fileNameToSizeWholeMap, String[] keyOrder,
      Map<Integer, SerializedNode> nodeInfo, int[] shardingIndexes,
      FieldTypeArrayDataDescription fieldDataDesc, String directoryLocation,
      String dataDirectoryLocation) {
    this.bucketCombinationToNodeNumberMap = bucketCombinationToNodeNumberMap;
    this.bucketToNodeNumberMap = bucketToNodeNumberMap;
    this.keyOrder = keyOrder;
    this.nodeInfo = nodeInfo;
    this.fileNameToNodeIdsCache = new HashMap<>();
    this.noOfDimensions = keyOrder.length;
    this.fileNameToSizeWholeMap = fileNameToSizeWholeMap;
    this.fileToNodeId = new HashMap<>();
    this.keyToBucketToFileList = new HashMap<>();
    this.shardingIndexes = shardingIndexes;
    this.fieldDataDesc = fieldDataDesc;
    this.directoryLocation = directoryLocation;
    this.hhFileSize = 0;
    this.dataDirectoryLocation = dataDirectoryLocation;
    initializeKeyToBucketToFileList();
    calculateBucketToFileMap("", 0);
  }

  public Map<BucketCombination, Set<Node>> getBucketCombinationToNodeNumberMap() {
    return bucketCombinationToNodeNumberMap;
  }

  public HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    return bucketToNodeNumberMap;
  }

  public String[] getKeyOrder() {
    return keyOrder;
  }

  public int[] getShardingIndexes() {
    return shardingIndexes;
  }

  public FieldTypeArrayDataDescription getFieldDataDesc() {
    return fieldDataDesc;
  }

  public String getAbsoluteDataFilePath() {
    return dataDirectoryLocation;
  }

  public static File getTemporaryDirectory() {
    if (tmpDirectory == null) {
      tmpDirectory = Files.createTempDir();
    }
    return tmpDirectory;
  }

  public Partition[] getPartitions(int id, int noOfExecutors, List<Integer> jobShardingDimensions,
      int jobPrimaryDimensionIdx, List<String> jobShardingDimensionsKey,
      String primaryDimensionKey) {
    int noOfShardingDimensions = keyOrder.length;
    int noOfEstimatedPartitions = 1;
    int[] jobShardingDimensionsArray = new int[jobShardingDimensions.size()];
    int i = 0;
    for (String shardingDimensionKey : jobShardingDimensionsKey) {
      int bucketSize = bucketToNodeNumberMap.get(shardingDimensionKey).size();
      noOfEstimatedPartitions = noOfEstimatedPartitions * bucketSize;
      jobShardingDimensionsArray[i] = jobShardingDimensions.get(i);
      // System.out.print(jobShardingDimensionsArray[i]);
      i++;

    }

    // System.out.println("");

    int[][] combinationArray = new int[noOfEstimatedPartitions][];
    populateCombination(combinationArray, null, 0, jobShardingDimensionsArray, 0);

    Partition[] partitions = null;
    if (noOfEstimatedPartitions <= noOfExecutors) {
      partitions = new HHRDDPartition[noOfEstimatedPartitions];
      for (int index = 0; index < noOfEstimatedPartitions; index++) {
        List<Tuple2<String, int[]>> files = new ArrayList<>();
        listFile(files, "", 0, noOfShardingDimensions, jobShardingDimensionsArray,
            combinationArray[index]);
        System.out.println();
        int preferredNodeId = bucketToNodeNumberMap.get(primaryDimensionKey)
            .get(new Bucket<>(combinationArray[index][jobPrimaryDimensionIdx])).getNodeId();
        List<String> preferredHosts = new ArrayList<>();
        preferredHosts.add(nodeInfo.get(preferredNodeId).getIp());
        partitions[index] =
            new HHRDDPartition(id, index, new File(this.directoryLocation).getPath(),
                this.fieldDataDesc, preferredHosts, files, nodeInfo);
      }

    } else {
      long idealPartitionFileSize = 128 * 1024 * 1024;
      PriorityQueue<PartitionBucket> partitionBuckets = new PriorityQueue<>();
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
      int partitionIdx = 0;
      Iterator<PartitionBucket> partitionBucketIterator = partitionBuckets.iterator();
      List<Partition> listOfPartitions = new ArrayList<>();
      while (partitionBucketIterator.hasNext()) {
        PartitionBucket partitionBucket1 = partitionBucketIterator.next();
        PriorityQueue<NodeBucket> nodeBuckets = new PriorityQueue<>();
        for (Map.Entry<Integer, NodeBucket> nodeBucketEntry : partitionBucket1.getNodeBucketMap()
            .entrySet()) {
          nodeBuckets.offer(nodeBucketEntry.getValue());
        }
        int maxNoOfPreferredNodes = getShardingIndexes().length;// No of Preferred Nodes
        int remNoOfPreferredNodes = maxNoOfPreferredNodes;
        NodeBucket nodeBucket;
        List<String> preferredIpList = new ArrayList<>();
        while ((nodeBucket = nodeBuckets.poll()) != null && remNoOfPreferredNodes > 0) {
          preferredIpList.add(nodeInfo.get(nodeBucket.getId()).getIp());
          remNoOfPreferredNodes--;
        }
        List<Tuple2<String, int[]>> files = partitionBucket1.getFiles();
        if (!files.isEmpty()) {
          Partition partition =
              new HHRDDPartition(id, partitionIdx, new File(this.directoryLocation).getPath(),
                  this.fieldDataDesc, preferredIpList, files, nodeInfo);
          partitionIdx++;
          listOfPartitions.add(partition);
        }
      }
      System.out.println("file count : " + fileCount);
      System.out.println("PartitionSize : " + listOfPartitions.size());
      partitions = new Partition[listOfPartitions.size()];
      for (int j = 0; j < partitions.length; j++) {
        partitions[j] = listOfPartitions.get(j);
      }

    }

    return partitions;
  }

  public Partition[] getOptimizedPartitions(int id, int noOfExecutors,
      List<Integer> jobShardingDimensions, int jobPrimaryDimensionIdx,
      List<String> jobShardingDimensionsKey, String primaryDimensionKey) {
    int totalCombination = fileNameToSizeWholeMap.size();
    System.out.println("jobShardingDimensions " + jobShardingDimensions);
    System.out.println("jobShardingDimensionsKey " + jobShardingDimensionsKey);
    Partition[] partitions;
    if (noOfExecutors < totalCombination) {
      long idealPartitionFileSize = 128 * 1024 * 1024;// 128MB partition size
      List<Partition> listOfPartitions = new ArrayList<>();
      int partitionIdx = 0;
      int fileCount = 0;

      PriorityQueue<PartitionBucket> partitionBuckets = new PriorityQueue<>();
      PartitionBucket partitionBucket = new PartitionBucket(0);
      partitionBuckets.offer(partitionBucket);
      Set<String> fileNamesSet = new HashSet<>();
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
      System.out.println("No of unique files : " + fileNamesSet.size());

      Iterator<PartitionBucket> partitionBucketIterator = partitionBuckets.iterator();
      while (partitionBucketIterator.hasNext()) {
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
          preferredIpList.add(nodeInfo.get(nodeBucket.getId()).getIp());
          remNoOfPreferredNodes--;
        }
        List<Tuple2<String, int[]>> files = partitionBucket1.getFiles();
        if (!files.isEmpty()) {
          System.out.println("Preferred Ips : " + preferredIpList.toString());
          Iterator<Tuple2<String, int[]>> itr = files.iterator();
          while (itr.hasNext()) {
            Tuple2<String, int[]> tuple = itr.next();
            /*
             * System.out .println(" Files  :: " + tuple._1 + " And node Ip : " +
             * Arrays.toString(tuple._2));
             */
          }
          Partition partition =
              new HHRDDPartition(id, partitionIdx, new File(this.directoryLocation).getPath(),
                  this.fieldDataDesc, preferredIpList, files, nodeInfo);
          partitionIdx++;
          listOfPartitions.add(partition);
        }
      }
      System.out.println("file count : " + fileCount);
      System.out.println("PartitionSize : " + listOfPartitions.size());
      partitions = new Partition[listOfPartitions.size()];
      for (int j = 0; j < partitions.length; j++) {
        partitions[j] = listOfPartitions.get(j);
      }

    } else {
      partitions = new Partition[totalCombination];
      int index = 0;
      for (Map.Entry<String, Tuple2<String, int[]>> fileEntry : fileToNodeId.entrySet()) {
        List<Tuple2<String, int[]>> files = new ArrayList<>();
        int preferredNodeId =
            fileEntry.getValue()._2[jobShardingDimensions.get(jobPrimaryDimensionIdx)];
        List<String> preferredHosts = new ArrayList<>();
        preferredHosts.add(nodeInfo.get(preferredNodeId).getIp());
        partitions[index] =
            new HHRDDPartition(id, index, new File(this.directoryLocation).getPath(),
                this.fieldDataDesc, preferredHosts, files, nodeInfo);
        index++;
      }
    }


    return partitions;
  }


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
      Set<Node> nodes = bucketCombinationToNodeNumberMap.get(bucketCombination);
      int i = 0;
      for (Node node : nodes) {
        nodeIds[i] = node.getNodeId();
        i++;
      }

      fileNameToNodeIdsCache.put(fileName, nodeIds);
    }
    return nodeIds;
  }


  private void initializeKeyToBucketToFileList() {
    for (int dim = 0; dim < keyOrder.length; dim++) {
      Map<Integer, List<String>> bucketToFileList = new HashMap<>();
      keyToBucketToFileList.put(keyOrder[dim], bucketToFileList);
      for (Bucket<KeyValueFrequency> bucket : bucketToNodeNumberMap.get(keyOrder[dim]).keySet()) {
        List<String> fileList = new ArrayList<>();
        bucketToFileList.put(bucket.getId(), fileList);
      }
    }
  }

  private void calculateBucketToFileMap(String fileName, int dim) {
    if (dim == noOfDimensions) {
      Tuple2<String, int[]> tuple2 = new Tuple2<>(fileName, getFileLocationNodeIds(fileName));
      fileToNodeId.put(fileName, tuple2);
      hhFileSize += fileNameToSizeWholeMap.get(fileName);
      String[] buckets = fileName.split("_");
      for (int i = 0; i < noOfDimensions; i++) {
        keyToBucketToFileList.get(keyOrder[i]).get(Integer.parseInt(buckets[i])).add(fileName);
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

  class PartitionBucket implements Comparable<PartitionBucket> {
    long size;
    List<Tuple2<String, int[]>> files;
    Map<Integer, NodeBucket> nodeBucketMap;

    public PartitionBucket(long size) {
      this.size = size;
      this.files = new ArrayList<>();
      this.nodeBucketMap = new HashMap<>();
    }

    @Override
    public int compareTo(PartitionBucket o) {
      return Long.compare(size, o.size);
    }

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

    public List<Tuple2<String, int[]>> getFiles() {
      return files;
    }

    public long getSize() {
      return size;
    }

    public Map<Integer, NodeBucket> getNodeBucketMap() {
      return nodeBucketMap;
    }
  }

  class NodeBucket implements Comparable<NodeBucket> {
    int id;
    long size;

    public NodeBucket(int id, long size) {
      this.id = id;
      this.size = size;
    }

    @Override
    public int compareTo(NodeBucket o) {
      return Long.compare(o.size, size);
    }

    private void addSize(long size) {
      this.size += size;
    }

    public long getSize() {
      return size;
    }

    public int getId() {
      return id;
    }
  }
}
