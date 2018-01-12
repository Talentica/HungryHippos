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

import java.util

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription
import com.talentica.hungryHippos.rdd.SerializedNode
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext
import com.talentica.hungryHippos.sharding.{Bucket, KeyValueFrequency, Node}
import com.talentica.hungryhippos.filesystem.{FileStatistics, SerializableComparator}
import org.apache.spark.Partition
import org.apache.spark.sql.sources.Filter

/**
  * Created by rajkishoreh.
  * The interface for interacting with HHRDD metadata information.
  */
trait HHRDDInfo {

  /**
    * Gets the sharding indexes.
    *
    * @return the sharding indexes
    */
  def getShardingIndexes: Array[Int]

  /**
    * Gets the key order.
    *
    * @return the key order
    */
  def getKeyOrder: Array[String]

  /**
    * Gets the optimized partitions.
    *
    * @param id                       the id
    * @param noOfExecutors            the no of executors
    * @param jobShardingDimensions    the job sharding dimensions
    * @param jobPrimaryDimensionIdx   the job primary dimension idx
    * @param jobShardingDimensionsKey the job sharding dimensions key
    * @param primaryDimensionKey      the primary dimension key
    * @return the optimized partitions
    */
  def getOptimizedPartitions(id: Int, noOfExecutors: Int, jobShardingDimensions: java.util.List[Integer], jobPrimaryDimensionIdx: Int, jobShardingDimensionsKey: java.util.List[String], primaryDimensionKey: String, filteredFiles: java.util.Set[String], filters: Array[Filter]): Array[Partition]

  def getFileNameToSizeWholeMap: java.util.Map[String, java.lang.Long]

  def getFileStatisticsMap: java.util.Map[String, FileStatistics]

  /**
    * Gets the bucket to node number map.
    *
    * @return the bucket to node number map
    */
  def getBucketToNodeNumberMap: util.HashMap[String, util.HashMap[Bucket[KeyValueFrequency], Node]]

  /**
    * Gets the field data desc.
    *
    * @return the field data desc
    */
  def getFieldDataDesc: FieldTypeArrayDataDescription

  def getNodeIdToIp:util.Map[Integer, SerializedNode]

  def getDirectoryLocation:String

  def getMaxRecordsPerBlock:Int

  def getContext:ShardingApplicationContext

  def getBlockStatisticsFolderPath:String

  def getSerializableComparators:util.ArrayList[SerializableComparator[_]]

  def getColumnNameToIdxMap :util.HashMap[String,Integer]
}

