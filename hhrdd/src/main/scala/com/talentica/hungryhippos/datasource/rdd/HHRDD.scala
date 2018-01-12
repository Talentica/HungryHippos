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

import com.talentica.hungryhippos.filesystem.SerializableComparator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.Seq

/**
  * Created by rajkishoreh.
  */
class HHRDD(@transient private val sc: SparkContext, @transient private val hhrddInfo: HHRDDInfo, jobDimensions: Array[Integer],
            filteredFiles: util.Set[String], filters: Array[Filter], colMap:Array[Int]) extends RDD[Row](sc, Nil) with Serializable{

  /** The temp dir attempts. */
  private val TEMP_DIR_ATTEMPTS = 10000

  private val nodeIdToIp = hhrddInfo.getNodeIdToIp

  private val fieldDataDesc= hhrddInfo.getFieldDataDesc

  private val directoryLocation = hhrddInfo.getDirectoryLocation

  private val maxRecordsPerBlock = hhrddInfo.getMaxRecordsPerBlock

  private val blockStatisticsFolderPath = hhrddInfo.getBlockStatisticsFolderPath

  private val prefixBlackListDir = "blacklist_"+System.currentTimeMillis + "-"

  private var blackListDir:File = null

  private val serializableComparators = hhrddInfo.getSerializableComparators

  private val columnNameToIdxMap = hhrddInfo.getColumnNameToIdxMap

  def populatePartitions: Array[Partition] ={
    val keyOrder = hhrddInfo.getKeyOrder
    val shardingIndexes = hhrddInfo.getShardingIndexes
    val jobShardingDimensions = new util.ArrayList[Integer]
    val jobShardingDimensionsKey = new util.ArrayList[String]


    var primaryDimensionKey = ""
    var jobPrimaryDimensionIdx = 0
    val maxBucketSize = 0
    var jobDimensionIdx = 0
    var keyOrderIdx = 0
    if (jobDimensions != null && jobDimensions.length > 0) for (i <- shardingIndexes.indices) {
      for (j <- jobDimensions.indices) {
        if (shardingIndexes(i) == jobDimensions(j)) {
          val bucketSize = hhrddInfo.getBucketToNodeNumberMap.get(keyOrder(i)).size
          val dimensionKey = keyOrder(i)
          jobShardingDimensions.add(shardingIndexes(i))
          jobShardingDimensionsKey.add(keyOrder(i))
          if (bucketSize > maxBucketSize) {
            keyOrderIdx = i
            primaryDimensionKey = dimensionKey
            jobPrimaryDimensionIdx = jobDimensionIdx
          }
          jobDimensionIdx += 1
        }
      }
    }
    if (jobShardingDimensions.isEmpty) {
      jobPrimaryDimensionIdx = shardingIndexes(0)
      primaryDimensionKey = keyOrder(0)
      jobShardingDimensions.add(jobPrimaryDimensionIdx)
      jobShardingDimensionsKey.add(primaryDimensionKey)
      jobDimensionIdx += 1
    }

    val noOfExecutors = sc.defaultParallelism
    hhrddInfo.getOptimizedPartitions(id, noOfExecutors, jobShardingDimensions, jobPrimaryDimensionIdx, jobShardingDimensionsKey, primaryDimensionKey + keyOrderIdx, filteredFiles, filters)
  }

  private val rddPartitions = populatePartitions

  override def compute(split: Partition, context: TaskContext) = {
    val tempDir = createTempDir
    val hhRDDPartition = split.asInstanceOf[HHRDDPartition]
    var iterator:HHRDDIterator = null
    try
      iterator = new HHRDDIterator(fieldDataDesc, directoryLocation, fieldDataDesc.getSize, hhRDDPartition.getFiles, nodeIdToIp, tempDir, filters, maxRecordsPerBlock, colMap,blockStatisticsFolderPath,serializableComparators, columnNameToIdxMap)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    iterator
  }

  override protected def getPartitions = this.rddPartitions

  /**
    * Creates the temp dir.
    *
    * @return the file
    */
  def createTempDir: File = {
    if(blackListDir!=null) return blackListDir
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    var counter = 0
    for ( counter <-0 until TEMP_DIR_ATTEMPTS) {
      val tempDir = new File(baseDir, prefixBlackListDir + counter)
      tempDir.deleteOnExit()
      if (tempDir.mkdir) {
        blackListDir = tempDir
        return blackListDir
      }
    }
    throw new IllegalStateException("Failed to create directory within " + TEMP_DIR_ATTEMPTS + " attempts (tried " + prefixBlackListDir + "0 to " + prefixBlackListDir + (TEMP_DIR_ATTEMPTS - 1) + ')')
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val nodes = new util.ArrayList[String]
    nodes.addAll(partition.asInstanceOf[HHRDDPartition].getPreferredHosts)
    if (nodes == null || nodes.isEmpty) return null
    scala.collection.JavaConversions.asScalaBuffer(nodes).seq
  }
}
