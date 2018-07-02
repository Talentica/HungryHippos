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

package com.talentica.hungryhippos.datasource.orc

import java.io.File
import java.util
import java.util.{Comparator, Map}

import com.talentica.hungryHippos.sharding.{Bucket, BucketCombination, KeyValueFrequency, Node}
import com.talentica.hungryHippos.utility.FileSystemConstants
import com.talentica.hungryhippos.config.cluster
import com.talentica.hungryhippos.filesystem.FileInfo
import com.talentica.hungryhippos.filesystem.context.FileSystemContext
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.Breaks._

class HHInMemoryFileIndexHelper(session: SparkSession,
                                hhFileAbsolutePath: String,
                                distributedPath: String,
                                schema:StructType,
                                keyOrder: Array[String],
                                metaDataMap: util.Map[String, FileInfo],
                                dimensionCol:String,
                                fileLocationInfo:util.Map[String, Array[Int]],
                                nodeIdToNode: util.Map[Integer, cluster.Node],
                                bucketToNodeNumberMap: util.HashMap[String, util.HashMap[Bucket[KeyValueFrequency], Node]]) extends Logging {

  private var rootPathsSpecified: Seq[Path] = _
  private val fileStatusCache = FileStatusCache.getOrCreate(session)
  private var leafFiles = populateFileNamesToNodeIds
  private var sizeInBytes: Long = 0


  def populateFileNamesToNodeIds: mutable.LinkedHashSet[FileStatus] = {
    var pathList = new util.ArrayList[Path]
    val output = mutable.LinkedHashSet[FileStatus]()
    val metaDataEntrySetIterator = asScalaIterator(metaDataMap.entrySet().iterator())
    var dimension = getDimension
    var totalSize :Long= 0l
    var flag = true
    while (metaDataEntrySetIterator.hasNext) {
      flag = false
      val metaDataEntry = metaDataEntrySetIterator.next()
      val fileName = metaDataEntry.getKey
      val fileDirectory = fileName.replaceFirst(File.separator + FileSystemConstants.ORC_MAIN_FILE_NAME, "")
        .replaceFirst(File.separator + FileSystemConstants.ORC_DELTA_FILE_NAME, "")

      val ids = fileLocationInfo.get(fileDirectory)
      val names = new Array[String](ids.length)
      val hosts = new Array[String](ids.length)


      val buckets: Array[String] = fileDirectory.split("_")

      val node = bucketToNodeNumberMap.get(keyOrder(dimension)).get(new Bucket[KeyValueFrequency](buckets(dimension).toInt))
      val primaryNodeId = node.getNodeId
      var idx = 0
      names(idx) = nodeIdToNode.get(primaryNodeId).getName
      hosts(idx) = nodeIdToNode.get(primaryNodeId).getIp
      for (nodeId <- ids) {
        if(nodeId!=primaryNodeId){
          val node = nodeIdToNode.get(nodeId)
          idx += 1
          names(idx) = node.getName
          hosts(idx) = node.getIp
        }
      }
      val metaData = metaDataEntry.getValue
      if (metaData.length() > 0) {
        totalSize += metaData.length()
        val blockLocation = new BlockLocation(names, hosts, 0, metaData.length())
        val blockLocations = new Array[BlockLocation](1)
        blockLocations(0) = blockLocation
        var path: Path = new Path("hhfs://" + distributedPath+File.separator
          + FileSystemContext.getDataFilePrefix+File.separator + fileName)
        val locatedFileStatus = new LocatedFileStatus(metaData.length(), false,
          hosts.length,
          metaData.length(), metaData.lastModified(), metaData.lastModified(),
          null, null, null,
          null, path,
          blockLocations)
        pathList.add(path)
        output += locatedFileStatus
        val fileStatusArray = new Array[FileStatus](1)
        fileStatusArray(0) = locatedFileStatus
/*         logInfo("locatedFileStatus "+locatedFileStatus.toString)
        logInfo("blockLocation "+blockLocation.toString)*/
        fileStatusCache.putLeafFiles(path, fileStatusArray)
        sizeInBytes += metaData.length()
      }
    }
    util.Collections.sort[Path](pathList, new Comparator[Path] {
      override def compare(x: Path, y: Path): Int = {
        val xParts = x.getParent.getName.split("_")
        val yParts = y.getParent.getName.split("_")
        xParts(dimension).compareTo(yParts(dimension))
      }
    })
    rootPathsSpecified = asScalaBuffer(pathList)
    logInfo("Total size : " + totalSize)
    output
  }

  def getRootPathsSpecified: Seq[Path] = {
    rootPathsSpecified
  }

  def getLeafFiles: mutable.LinkedHashSet[FileStatus] = {
    leafFiles
  }

  def getFileStatusCache: FileStatusCache = {
    fileStatusCache
  }

  def getSizeInBytes: Long = {
    sizeInBytes
  }
  def getDimension:Int = {
    breakable {
      for (i <- keyOrder.indices) {
        if (keyOrder(i).toLowerCase.equals(dimensionCol)) {
          return i
        }
      }
    }
    return 0
  }

  def getSchema:StructType = schema
}
