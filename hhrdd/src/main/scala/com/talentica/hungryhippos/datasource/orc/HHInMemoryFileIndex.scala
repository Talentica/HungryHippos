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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class HHInMemoryFileIndex(sparkSession: SparkSession,
                          parameters: Map[String, String],
                          partitionSchemaHH: Option[StructType],
                          helper : HHInMemoryFileIndexHelper,
                          dimension:Int)
  extends PartitioningAwareFileIndex(
    sparkSession, parameters, partitionSchemaHH, helper.getFileStatusCache) {


  override val rootPaths = helper.getRootPathsSpecified
  refresh0()

  @volatile private var cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus] = _
  @volatile private var cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]] = _
  @volatile private var cachedPartitionSpec: PartitionSpec = _

  def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = helper.getLeafFiles

  override def sizeInBytes: Long = super.sizeInBytes

  override protected val hadoopConf: Configuration =  sparkSession.sessionState.newHadoopConfWithOptions(parameters)

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]) = {

    val selectedPartitions = allFiles().
      filter(f => isDataPath(f.getPath)).groupBy(f=>{
      val fParts = f.getPath.getParent.getName.split("_")
      fParts(dimension)
    }).mapValues(value=>{
      PartitionDirectory(InternalRow.empty,value)
    }).map(entry=>{
      entry._2
    }).toSeq
    /*logInfo("Printing file status dimension "+dimension)
    var partitionId = 1
    for(x<-selectedPartitions){
      logInfo("PartitionDirectory "+partitionId)
      for(y<-x.files){
        logInfo(y.toString)
        logInfo(y.asInstanceOf[LocatedFileStatus].getBlockLocations()(0).toString)
      }
      partitionId+=1
    }*/
    selectedPartitions
    //super.listFiles(partitionFilters, dataFilters)
  }

  override def inputFiles = super.inputFiles

  override def allFiles() = super.allFiles()

  override protected def inferPartitioning() = {
    super.inferPartitioning()
  }

  override def partitionSchema = partitionSchemaHH.get

  override def equals(other: Any): Boolean = other match {
    case hhfs: HHInMemoryFileIndex => rootPaths.toSet == hhfs.rootPaths.toSet
    case _ => false
  }

  override def hashCode(): Int = rootPaths.toSet.hashCode()

  override def partitionSpec(): PartitionSpec = {
    if (cachedPartitionSpec == null) {
      cachedPartitionSpec = inferPartitioning()
    }
    logTrace(s"Partition spec: $cachedPartitionSpec")
    cachedPartitionSpec
  }

  private def refresh0(): Unit = {
    val files = listLeafFiles(rootPaths)
    cachedLeafFiles =
      new mutable.LinkedHashMap[Path, FileStatus]() ++= files.map(f => f.getPath -> f)
    cachedLeafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)
    cachedPartitionSpec = null
  }

  override def refresh(): Unit = {
    refresh0()
  }

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    cachedLeafFiles
  }

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    cachedLeafDirToChildrenFiles
  }

  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }
}
