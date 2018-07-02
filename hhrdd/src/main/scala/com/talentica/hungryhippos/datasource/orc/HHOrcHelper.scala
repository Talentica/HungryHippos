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

import java.io.{File, FileInputStream, IOException, ObjectInputStream}
import java.util
import javax.xml.bind.JAXBException

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil
import com.talentica.hungryHippos.rdd.SerializedNode
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext
import com.talentica.hungryHippos.sharding.util.{ShardingFileUtil, ShardingTableCopier}
import com.talentica.hungryHippos.utility.FileSystemConstants
import com.talentica.hungryhippos.config.cluster.Node
import com.talentica.hungryhippos.datasource.HHSchemaReader
import com.talentica.hungryhippos.datasource.orc.v2.HHRDDHelper
import com.talentica.hungryhippos.datasource.rdd.HHRDDHelper.getNodes
import com.talentica.hungryhippos.filesystem.FileInfo
import com.talentica.hungryhippos.filesystem.context.FileSystemContext
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object HHOrcHelper {

  /** The Constant bucketToNodeNumberMapFile. */
  val bucketToNodeNumberMapFile = "bucketToNodeNumberMap"

  /**
    * Gets the hhrdd info.
    *
    * @param distributedPath the distributed path
    * @return the hhrdd info
    * @throws JAXBException the JAXB exception
    * @throws IOException   Signals that an I/O exception has occurred.
    */
  @throws[JAXBException]
  @throws[IOException]
  def getHHInMemoryFileIndexHelper(sparkSession: SparkSession, distributedPath: String,
                                   dimensionCol: String): HHInMemoryFileIndexHelper = {
    val directoryLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemContext.getDataFilePrefix
    val metaDataLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME
    val metaDataFileLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemConstants.FILE_LOCATION_INFO
    val shardingFolderPath = FileSystemContext.getRootDirectory + distributedPath + File.separator + ShardingTableCopier.SHARDING_ZIP_FILE_NAME
    val bucketToNodeNumberMapFilePath = shardingFolderPath + File.separatorChar + bucketToNodeNumberMapFile
    var bucketToNodeNumberMap = ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFilePath)

    val context = new ShardingApplicationContext(shardingFolderPath)
    val clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache
    val nodes = clusterConfig.getNode
    val nodeIdToNodeMap = new util.HashMap[Integer, Node]
    val schema = HHSchemaReader.readSchema(context)

    import scala.collection.JavaConversions._
    for (node <- nodes) {
      nodeIdToNodeMap.put(node.getIdentifier, node)
    }
    val metaDataMap = readMetaData(metaDataLocation)
    val fileLocationInfo = readLocationMetaData(metaDataFileLocation)

    val hhInMemoryFileIndexHelper = new HHInMemoryFileIndexHelper(sparkSession, directoryLocation + File.separator,
      distributedPath, schema, context.getShardingDimensions,
      metaDataMap, dimensionCol, fileLocationInfo,
      nodeIdToNodeMap, bucketToNodeNumberMap)
    hhInMemoryFileIndexHelper
  }

  /**
    * Gets the hhrdd info.
    *
    * @param distributedPath the distributed path
    * @return the hhrdd info
    * @throws JAXBException the JAXB exception
    * @throws IOException   Signals that an I/O exception has occurred.
    */
  @throws[JAXBException]
  @throws[IOException]
  def getHHRDDHelper(sparkSession: SparkSession, distributedPath: String,
                     dimensionCol: String): HHRDDHelper = {
    val directoryLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemContext.getDataFilePrefix
    val metaDataLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME
    val metaDataFileLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemConstants.FILE_LOCATION_INFO
    val shardingFolderPath = FileSystemContext.getRootDirectory + distributedPath + File.separator + ShardingTableCopier.SHARDING_ZIP_FILE_NAME
    val bucketToNodeNumberMapFilePath = shardingFolderPath + File.separatorChar + bucketToNodeNumberMapFile
    var bucketToNodeNumberMap = ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFilePath)

    val context = new ShardingApplicationContext(shardingFolderPath)
    val clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache
    val nodeIdToNodeMap = new util.HashMap[Integer, SerializedNode]
    val schema = HHSchemaReader.readSchema(context)
    val nodes = getNodes(clusterConfig.getNode)

    import scala.collection.JavaConversions._
    for (node <- nodes) {
      nodeIdToNodeMap.put(node.getId, node)
    }
    val metaDataMap = readMetaData(metaDataLocation)
    val fileLocationInfo = readLocationMetaData(metaDataFileLocation)

    val hhrddHelper = new HHRDDHelper(sparkSession, schema, directoryLocation,
      distributedPath, context.getShardingDimensions,
      metaDataMap, dimensionCol, fileLocationInfo,
      nodeIdToNodeMap, bucketToNodeNumberMap)
    hhrddHelper
  }

  /**
    * Read meta data.
    *
    * @param metadataLocation the metadata location
    * @return the map
    * @throws IOException Signals that an I/O exception has occurred.
    */
  @throws[IOException]
  def readMetaData(metadataLocation: String): util.Map[String, FileInfo] = {
    val fileNameToFileInfosMap = new util.HashMap[String, FileInfo]
    val metadataFolder = new File(metadataLocation)
    if (metadataFolder.listFiles != null) {
      for (file <- metadataFolder.listFiles) {
        var objectInputStream: ObjectInputStream = null
        try {
          objectInputStream = new ObjectInputStream(new FileInputStream(file))
          val fileNameToSizeMap = objectInputStream.readObject.asInstanceOf[util.Map[String, FileInfo]]
          fileNameToFileInfosMap.putAll(fileNameToSizeMap)
        } catch {
          case e: ClassNotFoundException =>
            e.printStackTrace()
            throw new RuntimeException("Corrupted Metadata : " + e)
        } finally if (objectInputStream != null) try
          objectInputStream.close()
        catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
    else throw new RuntimeException(metadataLocation + " not a valid path")
    fileNameToFileInfosMap
  }

  def readLocationMetaData(path: String): util.Map[String, Array[Int]] = {
    var fileNameToNodeId: util.Map[String, Array[Int]] = null
    var objectInputStream: ObjectInputStream = null
    try {
      objectInputStream = new ObjectInputStream(new FileInputStream(path))
      fileNameToNodeId = objectInputStream.readObject.asInstanceOf[util.Map[String, Array[Int]]]
    } catch {
      case e@(_: IOException | _: ClassNotFoundException) =>
        e.printStackTrace()
        throw new RuntimeException(e)
    } finally {
      if (objectInputStream != null) objectInputStream.close()
    }
    fileNameToNodeId
  }


}
