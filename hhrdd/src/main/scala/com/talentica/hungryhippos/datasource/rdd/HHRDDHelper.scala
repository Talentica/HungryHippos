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

import java.io._
import java.util
import javax.xml.bind.JAXBException

import com.talentica.hungryHippos.coordination.HungryHippoCurator
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil
import com.talentica.hungryHippos.rdd.SerializedNode
import com.talentica.hungryHippos.rdd.utility.JaxbUtil
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext
import com.talentica.hungryHippos.sharding.util.{ShardingFileUtil, ShardingTableCopier}
import com.talentica.hungryHippos.utility.FileSystemConstants
import com.talentica.hungryhippos.config.client.ClientConfig
import com.talentica.hungryhippos.config.cluster.Node
import com.talentica.hungryhippos.filesystem.{FileStatistics, SerializableComparator}
import com.talentica.hungryhippos.filesystem.context.FileSystemContext


/**
  * Created by rajkishoreh.
  */
object HHRDDHelper {
  /** The Constant bucketCombinationToNodeNumbersMapFile. */
  val bucketCombinationToNodeNumbersMapFile = "bucketCombinationToNodeNumbersMap"
  /** The Constant bucketToNodeNumberMapFile. */
  val bucketToNodeNumberMapFile = "bucketToNodeNumberMap"

  /**
    * Initialize.
    *
    * @param clientConfigPath the client config path
    * @throws JAXBException         the JAXB exception
    * @throws FileNotFoundException the file not found exception
    */
  @throws[JAXBException]
  @throws[FileNotFoundException]
  def initialize(clientConfigPath: String): String = {
    val clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, classOf[ClientConfig])
    val servers = clientConfig.getCoordinationServers.getServers
    HungryHippoCurator.getInstance(servers)
    servers
  }

  /**
    * Gets the actual path.
    *
    * @param path the path
    * @return the actual path
    */
  def getActualPath(path: String): String = FileSystemContext.getRootDirectory + path

  /**
    * Read meta data.
    *
    * @param metadataLocation the metadata location
    * @return the map
    * @throws IOException Signals that an I/O exception has occurred.
    */
  @throws[IOException]
  def readMetaData(metadataLocation: String): util.Map[String, java.lang.Long] = {
    val fileNameToSizeWholeMap = new util.HashMap[String, java.lang.Long]
    val metadataFolder = new File(metadataLocation)
    if (metadataFolder.listFiles != null) for (file <- metadataFolder.listFiles) {
      var objectInputStream:ObjectInputStream = null
      try {
        objectInputStream = new ObjectInputStream(new FileInputStream(file))
        val fileNametoSizeMap = objectInputStream.readObject.asInstanceOf[util.Map[String, java.lang.Long]]
        fileNameToSizeWholeMap.putAll(fileNametoSizeMap)
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
    else throw new RuntimeException(metadataLocation + " not a valid path")
    System.out.println("No of Files : " + fileNameToSizeWholeMap.size)
    fileNameToSizeWholeMap
  }

  /**
    * Read meta data.
    *
    * @param fileStatisticsLocation the metadata location
    * @return the map
    * @throws IOException Signals that an I/O exception has occurred.
    */
  @throws[IOException]
  def readNodeFileStatistics(fileStatisticsLocation: String): util.Map[Integer, util.Map[String, FileStatistics]] = {
    val nodeFileStatisticsMap = new util.HashMap[Integer, util.Map[String, FileStatistics]]
    val fileStatisticsFolder = new File(fileStatisticsLocation)
    if (fileStatisticsFolder.listFiles != null) for (file <- fileStatisticsFolder.listFiles) {
      var objectInputStream:ObjectInputStream = null
      try {
        objectInputStream = new ObjectInputStream(new FileInputStream(file))
        val fileNametoSizeMap = objectInputStream.readObject.asInstanceOf[util.Map[String, FileStatistics]]
        nodeFileStatisticsMap.put(file.getName.toInt, fileNametoSizeMap)
      } catch {
        case e: ClassNotFoundException =>
          e.printStackTrace()
          throw new RuntimeException("Corrupted FileStatistics : " + e)
      } finally if (objectInputStream != null) try
        objectInputStream.close()
      catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
    else throw new RuntimeException(fileStatisticsLocation + " not a valid path")
    System.out.println("No of Files : " + nodeFileStatisticsMap.size)
    nodeFileStatisticsMap
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
  def getHhrddInfo(distributedPath: String): HHRDDInfoImpl = {
    val directoryLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemContext.getDataFilePrefix
    val metaDataLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME
    val blockStatisticsFolderPath = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemConstants.BLOCK_STATISTICS_FOLDER_NAME+File.separator
    val fileStatisticsLocation = FileSystemContext.getRootDirectory + distributedPath + File.separator + FileSystemConstants.FILE_STATISTICS_FOLDER_NAME
    val shardingFolderPath = FileSystemContext.getRootDirectory + distributedPath + File.separator + ShardingTableCopier.SHARDING_ZIP_FILE_NAME
    val context = new ShardingApplicationContext(shardingFolderPath)
    val dataDescription = context.getConfiguredDataDescription
    val clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache
    val nodes = getNodes(clusterConfig.getNode)
    val shardingIndexes = context.getShardingIndexes
    val bucketToNodeNumberMapFilePath = shardingFolderPath + File.separatorChar + bucketToNodeNumberMapFile
    val nodIdToIp = new util.HashMap[Integer, SerializedNode]
    val maxRecordsPerBlock = context.getShardingClientConfig.getMaximumSizeOfSingleBlockData
    val serializableComparators= context.getSerializableComparators
    import scala.collection.JavaConversions._
    for (serializedNode <- nodes) {
      nodIdToIp.put(serializedNode.getId, serializedNode)
    }
    val fileNameToSizeWholeMap = readMetaData(metaDataLocation)
    val nodeFileStatisticsMap = readNodeFileStatistics(fileStatisticsLocation)
    var serializedComparatorsList= new util.ArrayList[SerializableComparator[_]]()
    for(serializableComparator<-serializableComparators){
      serializedComparatorsList.add(serializableComparator)
    }
    val columnNameToIdxMap = context.getColumnsConfiguration
    val hhrddInfo = new HHRDDInfoImpl(ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFilePath), fileNameToSizeWholeMap, nodeFileStatisticsMap,
      context.getShardingDimensions, nodIdToIp, shardingIndexes, dataDescription, directoryLocation,maxRecordsPerBlock,context,blockStatisticsFolderPath, serializedComparatorsList,columnNameToIdxMap)
    hhrddInfo
  }

  /**
    * Gets the nodes.
    *
    * @param nodes the nodes
    * @return the nodes
    */
  def getNodes(nodes: util.List[Node]): util.List[SerializedNode] = {
    val serializedNodes = new util.ArrayList[SerializedNode]
    import scala.collection.JavaConversions._
    for (node <- nodes) {
      serializedNodes.add(new SerializedNode(node.getIdentifier, node.getIp, Integer.valueOf(node.getPort)))
    }
    serializedNodes
  }
}

