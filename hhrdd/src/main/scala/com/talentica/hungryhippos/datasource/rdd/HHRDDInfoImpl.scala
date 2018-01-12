/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryhippos.datasource.rdd

import java.io.File
import java.util
import java.util.{Collections, PriorityQueue}

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription
import com.talentica.hungryHippos.rdd.SerializedNode
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext
import com.talentica.hungryHippos.sharding.util.NodeSelector
import com.talentica.hungryHippos.sharding.{Bucket, BucketCombination, KeyValueFrequency, Node}
import com.talentica.hungryhippos.datasource.rdd
import com.talentica.hungryhippos.filesystem.{FileStatistics, SerializableComparator}
import org.apache.spark.Partition
import org.apache.spark.sql.sources.Filter

/**
  * Created by rajkishoreh.
  */
object HHRDDInfoImpl {

  private[rdd] class PartitionBucket(var size: Long)
    extends Comparable[HHRDDInfoImpl.PartitionBucket] {
    private[rdd] val fileDetails = new util.ArrayList[FileDetail]
    private[rdd] val nodeBucketMap = new util.HashMap[Integer, HHRDDInfoImpl.NodeBucket]

    override def compareTo(o: HHRDDInfoImpl.PartitionBucket): Int = java.lang.Long.compare(size, o.size)

    /**
      * Adds the file.
      *
      * @param fileDetail the file
      * @param size the size
      */
    def addFile(fileDetail: FileDetail, size: Long): Unit = {
      fileDetails.add(fileDetail)
      val nodeIds: Array[Int] = fileDetail.nodeIds
      var i: Int = 0
      for ( i <- 0 until nodeIds.length ) {
        val nodeId: Int = nodeIds(i)
        var nodeBucket: HHRDDInfoImpl.NodeBucket = nodeBucketMap.get(nodeId)
        if (nodeBucket == null) {
          nodeBucket = new HHRDDInfoImpl.NodeBucket(nodeId, 0)
          nodeBucketMap.put(nodeId, nodeBucket)
        }
        nodeBucket.addSize(size)
      }
      this.size += size
    }

    /**
      * Gets the files.
      *
      * @return the files
      */
    def getFiles: util.List[FileDetail] = fileDetails

    /**
      * Gets the size.
      *
      * @return the size
      */
    def getSize: Long = size

    /**
      * Gets the node bucket map.
      *
      * @return the node bucket map
      */
    def getNodeBucketMap: util.Map[Integer, HHRDDInfoImpl.NodeBucket] = nodeBucketMap
  }

  /**
    * The Class NodeBucket.
    */
  private[rdd] class NodeBucket(var id: Int, var size: Long)

    extends Comparable[HHRDDInfoImpl.NodeBucket] {
    override def compareTo(o: HHRDDInfoImpl.NodeBucket): Int = java.lang.Long.compare(o.size, size)

    /**
      * Adds the size.
      *
      * @param size the size
      */
    def addSize(size: Long): Unit = {
      this.size += size
    }

    def getSize: Long = size

    override def toString: String = " id: "+id+" size: "+size

    /**
      * Gets the id.
      *
      * @return the id
      */
    def getId: Int = id
  }

}

/**
  * Created by rajkishoreh.
  */
class HHRDDInfoImpl(/** The bucket to node number map. */
                    var bucketToNodeNumberMap: util.HashMap[String, util.HashMap[Bucket[KeyValueFrequency], Node]],

                    /** The file name to size whole map. */
                    var fileNameToSizeWholeMap: util.Map[String, java.lang.Long], var nodeFileStatisticsMap: util.Map[Integer, util.Map[String, FileStatistics]],

                    /** The key order. */
                    var keyOrder: Array[String],

                    /** The nod id to ip. */
                    var nodIdToIp: util.Map[Integer, SerializedNode],

                    /** The sharding indexes. */
                    var shardingIndexes: Array[Int],

                    /** The field data desc. */
                    var fieldDataDesc: FieldTypeArrayDataDescription,

                    /** The directory location. */
                    var directoryLocation: String,

                    var maxRecordsPerBlock:Int,

                    val context:ShardingApplicationContext,

                    var blockStatisticsFolderPath:String,

                    val serializableComparators:util.ArrayList[SerializableComparator[_]],
                    val columnNameToIdxMap:util.HashMap[String,Integer])

  extends HHRDDInfo { //this.bucketCombinationToNodeNumberMap = bucketCombinationToNodeNumberMap;
  private val nodeSelector = new NodeSelector
  private val fileNameToNodeIdsCache = new util.HashMap[String, Array[Int]]
  private val fileNameToBlockStatisticsLocation = new util.HashMap[String, Int]
  private val noOfDimensions = keyOrder.length
  private val fileToNodeId = new util.HashMap[String, FileDetail]
  private val keyToBucketToFileList = new util.HashMap[String, util.Map[Integer, util.List[String]]]
  private var hhFileSize = 0L
  private val fileStatisticsMap = new util.HashMap[String, FileStatistics]
  populateStatisticsMap
  def populateStatisticsMap: Unit = {
    val mapIterator: util.Iterator[util.Map[String, FileStatistics]] = nodeFileStatisticsMap.values.iterator
    while (mapIterator.hasNext) fileStatisticsMap.putAll(mapIterator.next)
  }

  initializeKeyToBucketToFileList()
  //calculateBucketToFileMap("", 0)
  calculateBucketToFileMap

  override def getBucketToNodeNumberMap: util.HashMap[String, util.HashMap[Bucket[KeyValueFrequency], Node]] = bucketToNodeNumberMap

  override def getKeyOrder: Array[String] = keyOrder

  override def getShardingIndexes: Array[Int] = shardingIndexes

  override def getFieldDataDesc: FieldTypeArrayDataDescription = fieldDataDesc

  override def getFileNameToSizeWholeMap: util.Map[String, java.lang.Long] = fileNameToSizeWholeMap

  override def getFileStatisticsMap: util.Map[String, FileStatistics] = fileStatisticsMap

  override def getNodeIdToIp:util.Map[Integer, SerializedNode] = nodIdToIp

  override def getDirectoryLocation:String = directoryLocation+File.separator

  override def getMaxRecordsPerBlock:Int = maxRecordsPerBlock

  override def getContext:ShardingApplicationContext = context

  override def getBlockStatisticsFolderPath:String = blockStatisticsFolderPath

  override def getSerializableComparators:util.ArrayList[SerializableComparator[_]]=serializableComparators

  override def getColumnNameToIdxMap :util.HashMap[String,Integer]= columnNameToIdxMap
  /**
    * Adds the partition to list and get partition idx.
    *
    * @param id                      the id
    * @param partitionIdx            the partition idx
    * @param partitionBucketIterator the partition bucket iterator
    * @param listOfPartitions        the list of partitions
    * @return the int
    */
  private def addPartitionToListAndGetPartitionIdx(id: Int, partitionIdx: Int, partitionBucketIterator: util.Iterator[HHRDDInfoImpl.PartitionBucket], listOfPartitions: util.List[Partition], filters: Array[Filter]): Int = {
    val partitionBucket1: HHRDDInfoImpl.PartitionBucket = partitionBucketIterator.next
    val nodeBuckets: PriorityQueue[HHRDDInfoImpl.NodeBucket] = new PriorityQueue[HHRDDInfoImpl.NodeBucket]
    var partitionIdxL = partitionIdx;
    import scala.collection.JavaConversions._
    for (nodeBucketEntry <- partitionBucket1.getNodeBucketMap.entrySet) {
      nodeBuckets.offer(nodeBucketEntry.getValue)
    }
    val maxNoOfPreferredNodes: Int = 3
    // No of Preferred Nodes
    var remNoOfPreferredNodes: Int = maxNoOfPreferredNodes
    var nodeBucket: HHRDDInfoImpl.NodeBucket = null
    val preferredIpList: util.List[String] = new util.ArrayList[String]
    //val sb = new StringBuilder()
    while (!nodeBuckets.isEmpty && remNoOfPreferredNodes > 0) {
      nodeBucket = nodeBuckets.poll
      preferredIpList.add(nodIdToIp.get(nodeBucket.getId).getIp)
      remNoOfPreferredNodes -= 1
      //sb.append(nodeBucket.toString).append(" ")
    }
   // System.out.println("partition idx : "+partitionIdxL+" size : "+partitionBucket1.size+" { "+sb.toString()+" }")
    val files: util.List[FileDetail] = partitionBucket1.getFiles
    if (!files.isEmpty) {
      val partition: Partition = new rdd.HHRDDPartition(id, partitionIdxL, preferredIpList, files)
      partitionIdxL += 1
      listOfPartitions.add(partition)
    }
    partitionIdxL
  }

  override def getOptimizedPartitions(id: Int, noOfExecutors: Int, jobShardingDimensions: util.List[Integer], jobPrimaryDimensionIdx: Int, jobShardingDimensionsKey: util.List[String], primaryDimensionKey: String, filteredFiles: util.Set[String], filters: Array[Filter]): Array[Partition] = {
    System.out.println("Called getOptimizedPartitions")
    val totalCombination: Int = fileStatisticsMap.size()
    System.out.println("jobShardingDimensions " + jobShardingDimensions)
    System.out.println("jobShardingDimensionsKey " + jobShardingDimensionsKey)
    var partitions: Array[Partition] = null
    /*
    if (noOfExecutors < totalCombination) partitions = getOptimizedPartitionsWhenTotalCombinationIsMore(id, primaryDimensionKey, filteredFiles, filters)
    else partitions = getOptimizedPartitionsWhenTotalCombinationIsLess(id, jobShardingDimensions, jobPrimaryDimensionIdx, totalCombination, filteredFiles, filters)
    */
    partitions = getOptimizedPartitionsWhenTotalCombinationIsMore(id, primaryDimensionKey, filteredFiles, filters)
    partitions
  }

  /**
    * Gets the optimized partitions when total combination is more.
    *
    * @param id                  the id
    * @param primaryDimensionKey the primary dimension key
    * @return the optimized partitions when total combination is more
    */
  private def getOptimizedPartitionsWhenTotalCombinationIsMore(id: Int, primaryDimensionKey: String, filteredFiles: util.Set[String], filters: Array[Filter]): Array[Partition] = {
    System.out.println("Called getOptimizedPartitionsWhenTotalCombinationIsMore")
    System.out.println("Total Number of files:" + fileStatisticsMap.size() + " Number of filtered files:" + filteredFiles.size)
    val idealPartitionFileSize: Long = 128 * 1024 * 1024
    // 128MB partition size
    val listOfPartitions: util.List[Partition] = new util.ArrayList[Partition]
    var partitionIdx: Int = 0
    val fileNamesSet: util.Set[String] = new util.HashSet[String]
    val partitionBuckets: PriorityQueue[HHRDDInfoImpl.PartitionBucket] = new PriorityQueue[HHRDDInfoImpl.PartitionBucket]
    preparePartitionsV2(primaryDimensionKey, idealPartitionFileSize, fileNamesSet, partitionBuckets, filteredFiles)
    System.out.println("Number of unique files : " + fileNamesSet.size)
    val partitionBucketIterator: util.Iterator[HHRDDInfoImpl.PartitionBucket] = partitionBuckets.iterator
    while (partitionBucketIterator.hasNext) partitionIdx = addPartitionToListAndGetPartitionIdx(id, partitionIdx, partitionBucketIterator, listOfPartitions, filters)
    System.out.println("PartitionSize : " + listOfPartitions.size)
    System.out.println("Max partitionIdx " + partitionIdx)
    val partitions: Array[Partition] = new Array[Partition](listOfPartitions.size)
    for (j <- partitions.indices) {
      partitions(j) = listOfPartitions.get(j)
    }
    partitions
  }

  /**
    * Prepare partitions and get file count.
    *
    * @param primaryDimensionKey    the primary dimension key
    * @param idealPartitionFileSize the ideal partition file size
    * @param fileNamesSet           the file names set
    * @param partitionBuckets       the partition buckets
    */
  private def preparePartitions(primaryDimensionKey: String, idealPartitionFileSize: Long, fileNamesSet: util.Set[String], partitionBuckets: PriorityQueue[HHRDDInfoImpl.PartitionBucket], filteredFiles: util.Set[String]): Unit = {
    var partitionBucket: HHRDDInfoImpl.PartitionBucket = new HHRDDInfoImpl.PartitionBucket(0)
    partitionBuckets.offer(partitionBucket)
    var fileCount: Int = 0
    import scala.collection.JavaConversions._
    for (entry <- keyToBucketToFileList.get(primaryDimensionKey).entrySet) {
      import scala.collection.JavaConversions._
      for (fileName <- entry.getValue) {
        val fileSize = fileStatisticsMap.get(fileName).getDataSize
        if (fileSize > 0 && filteredFiles.contains(fileName)) {
          partitionBucket = partitionBuckets.poll
          if (partitionBucket.getSize + fileSize > idealPartitionFileSize && partitionBucket.getSize != 0) {
            partitionBuckets.offer(partitionBucket)
            partitionBucket = new HHRDDInfoImpl.PartitionBucket(0)
          }
          partitionBucket.addFile(fileToNodeId.get(fileName), fileSize)
          partitionBuckets.offer(partitionBucket)
          fileNamesSet.add(fileName)
          fileCount += 1
        }
      }
    }
    System.out.println("file count : " + fileCount)
  }

  /**
    * Prepare partitions and get file count.
    *
    * @param primaryDimensionKey    the primary dimension key
    * @param idealPartitionFileSize the ideal partition file size
    * @param fileNamesSet           the file names set
    * @param partitionBuckets       the partition buckets
    */
  private def preparePartitionsV2(primaryDimensionKey: String, idealPartitionFileSize: Long, fileNamesSet: util.Set[String], partitionBuckets: PriorityQueue[HHRDDInfoImpl.PartitionBucket], filteredFiles: util.Set[String]): Unit = {
    var currPartitionBucket:HHRDDInfoImpl.PartitionBucket = null
    var fileCount: Int = 0
    import scala.collection.JavaConversions._
    for (entry <- keyToBucketToFileList.get(primaryDimensionKey).entrySet) {
      currPartitionBucket = new HHRDDInfoImpl.PartitionBucket(0)
      import scala.collection.JavaConversions._
      for (fileName <- entry.getValue) {
        val fileSize = fileStatisticsMap.get(fileName).getDataSize
        if (fileSize > 0 && filteredFiles.contains(fileName)) {
          if (currPartitionBucket.getSize + fileSize > idealPartitionFileSize && currPartitionBucket.getSize != 0) {
            partitionBuckets.offer(currPartitionBucket)
            currPartitionBucket = new HHRDDInfoImpl.PartitionBucket(0)
          }
          currPartitionBucket.addFile(fileToNodeId.get(fileName), fileSize)
          fileNamesSet.add(fileName)
          fileCount += 1
        }
      }
      if(currPartitionBucket.size>0){
        partitionBuckets.offer(currPartitionBucket)
      }
    }

    System.out.println("file count : " + fileCount)
  }


  /**
    * Gets the optimized partitions when total combination is less.
    *
    * @param id                     the id
    * @param jobShardingDimensions  the job sharding dimensions
    * @param jobPrimaryDimensionIdx the job primary dimension idx
    * @param totalCombination       the total combination
    * @param filteredFiles
    * @return the optimized partitions when total combination is less
    */
  private def getOptimizedPartitionsWhenTotalCombinationIsLess(id: Int, jobShardingDimensions: util.List[Integer], jobPrimaryDimensionIdx: Int, totalCombination: Int, filteredFiles: util.Set[String], filters: Array[Filter]): Array[Partition] = {
    System.out.println("Called getOptimizedPartitionsWhenTotalCombinationIsLess")
    val partitionList: util.List[Partition] = new util.ArrayList[Partition]
    var index: Int = 0
    import scala.collection.JavaConversions._
    for (fileEntry <- fileToNodeId.entrySet) {
      if (fileStatisticsMap.get(fileEntry.getKey).getDataSize > 0 && filteredFiles.contains(fileEntry.getKey)) {
        val files = new util.ArrayList[FileDetail]
        files.add(fileEntry.getValue)
        val preferredHosts: util.List[String] = new util.ArrayList[String]
        for(preferredNodeId<-fileEntry.getValue.nodeIds){
          preferredHosts.add(nodIdToIp.get(preferredNodeId).getIp)
        }
        partitionList.add(new rdd.HHRDDPartition(id, index, preferredHosts, files))
        index += 1
      }
    }
    val partitions: Array[Partition] = new Array[Partition](partitionList.size)
    for (i <- partitions.indices) {
      partitions(i) = partitionList.get(i)
    }
    partitions
  }

  def getFileLocationNodeIds(fileName: String): Array[Int] = {
    var nodeIds: Array[Int] = fileNameToNodeIdsCache.get(fileName)
    if (nodeIds == null) {
      nodeIds = new Array[Int](keyOrder.length)
      val buckets: Array[String] = fileName.split("_")
      val keyValueCombination: util.Map[String, Bucket[KeyValueFrequency]] = new util.HashMap[String, Bucket[KeyValueFrequency]]

      for (i <- keyOrder.indices) {
        keyValueCombination.put(keyOrder(i), new Bucket[KeyValueFrequency](buckets(i).toInt))
      }
      val bucketCombination: BucketCombination = new BucketCombination(keyValueCombination)
      val ids: util.Set[Integer] = nodeSelector.selectNodeIds(bucketCombination, bucketToNodeNumberMap, keyOrder)
      var i: Int = 0
      import scala.collection.JavaConversions._
      for (nodeId <- ids) {
        nodeIds(i) = nodeId
        i += 1
      }

      fileNameToBlockStatisticsLocation.put(fileName,bucketToNodeNumberMap.get(keyOrder(0)).get(bucketCombination.getBucketsCombination.get(keyOrder(0))).getNodeId)
      fileNameToNodeIdsCache.put(fileName, nodeIds)
    }
    return nodeIds
  }

  /**
    * Initialize key to bucket to file list.
    */
  def initializeKeyToBucketToFileList(): Unit = {
    var dim: Int = 0
    for (dim <- keyOrder.indices) {
      val bucketToFileList: util.Map[Integer, util.List[String]] = new util.HashMap[Integer, util.List[String]]
      keyToBucketToFileList.put(keyOrder(dim) + dim, bucketToFileList)
      import scala.collection.JavaConversions._
      for (bucket <- bucketToNodeNumberMap.get(keyOrder(dim)).keySet) {
        val fileList: util.List[String] = new util.ArrayList[String]
        bucketToFileList.put(bucket.getId, fileList)
      }
    }
  }

  /**
    * Calculate bucket to file map.
    */
  def calculateBucketToFileMap: Unit = {
    import scala.collection.JavaConversions._
    for (bucket <- bucketToNodeNumberMap.get(keyOrder(0)).keySet) {
      val shardDuplicates = new util.HashMap[String,Integer]()
      shardDuplicates.put(keyOrder(0),bucket.getId)
      calculateBucketToFileMapUtil(bucket.getId + "", 1,shardDuplicates)
    }
  }

  def calculateBucketToFileMapUtil(fileName: String, dim: Int,shardDuplicates:util.HashMap[String,Integer]): Unit = {
    if (dim == noOfDimensions) {
      var nodeIds = getFileLocationNodeIds(fileName);
      val fileDetail= new FileDetail(fileName, nodeIds,fileStatisticsMap.get(fileName).getDataSize, fileNameToBlockStatisticsLocation.get(fileName))
      fileToNodeId.put(fileName, fileDetail)
      hhFileSize += fileStatisticsMap.get(fileName).getDataSize
      val buckets: Array[String] = fileName.split("_")
      for (i <- 0 until noOfDimensions) {
        keyToBucketToFileList.get(keyOrder(i) + i).get(buckets(i).toInt).add(fileName)
      }
      return
    }
    if(shardDuplicates.get(keyOrder(dim))!=null){
      calculateBucketToFileMapUtil(fileName + "_" + shardDuplicates.get(keyOrder(dim)), dim + 1,shardDuplicates)
    }else{
      import scala.collection.JavaConversions._
      for (bucket <- bucketToNodeNumberMap.get(keyOrder(dim)).keySet) {
        shardDuplicates.put(keyOrder(dim),bucket.getId)
        calculateBucketToFileMapUtil(fileName + "_" + bucket.getId, dim + 1,shardDuplicates)
      }
      shardDuplicates.remove(keyOrder(dim))
    }

  }
  /*
  * Calculate bucket to file map.
  *
  * @param fileName the file name
  * @param dim      the dim
    */
  def calculateBucketToFileMap2: Unit = {
   val fileNames = new util.ArrayList[String](fileStatisticsMap.keySet())
   Collections.sort(fileNames)
    import scala.collection.JavaConversions._
    for(fileName <- fileNames){
      if(fileStatisticsMap.get(fileName).getDataSize>0){
        var nodeIds = getFileLocationNodeIds(fileName)
        val fileDetail= new FileDetail(fileName, nodeIds,fileStatisticsMap.get(fileName).getDataSize, fileNameToBlockStatisticsLocation.get(fileName))
        fileToNodeId.put(fileName, fileDetail)
        hhFileSize += fileStatisticsMap.get(fileName).getDataSize
        val buckets: Array[String] = fileName.split("_")
        for (i <- 0 until noOfDimensions) {
          keyToBucketToFileList.get(keyOrder(i) + i).get(buckets(i).toInt).add(fileName)
        }
      }

    }
  }

}