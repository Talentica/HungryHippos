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
import java.nio.ByteBuffer
import java.util
import java.util.Collections

import com.talentica.hungryHippos.client.domain.DataDescription
import com.talentica.hungryHippos.rdd.SerializedNode
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader
import com.talentica.hungryHippos.utility.FileSystemConstants
import com.talentica.hungryhippos.datasource.HHBlockFilterUtility
import com.talentica.hungryhippos.filesystem.{BlockStatistics, SerializableComparator}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.slf4j.LoggerFactory
import org.xerial.snappy.SnappyInputStream

import scala.collection.AbstractIterator
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

/**
  * Created by rajkishoreh.
  */
class HHRDDIterator(dataDescription: DataDescription, filePath: String, recordLength: Int, files: util.List[FileDetail], nodeInfo: util.Map[Integer, SerializedNode],
                    blackListDir: File, filters: Array[Filter], maxRecordsPerBlock: Int, colMap: Array[Int], blockStatisticsFolderPath: String, serializableComparators:util.ArrayList[SerializableComparator[_]], columnNameToIdxMap:util.HashMap[String,Integer]) extends AbstractIterator[Row] {
  private val logger = LoggerFactory.getLogger(classOf[HHRDDIterator])

  val remoteFilesSet = new util.HashSet[String]
  val blackListedIps = new util.HashSet[String]
  val tmpDownloadDir = HHRDDIteratorHelper.createTempDir
  val tmpDownloadPath = tmpDownloadDir.getAbsolutePath + File.separator
  val noOfReplicas = files.get(0).nodeIds.length




  val fileToBeDownloaded = new util.HashMap[String, FileDetail]
  val nodeToFileMap = new util.HashMap[Integer, util.List[FileDetail]]

  def markRemoteFiles = {
    var nodeInfoItr = asScalaIterator(nodeInfo.keySet().iterator())
    nodeInfoItr.foreach(key => {
      nodeToFileMap.put(key, new util.LinkedList[FileDetail]())
    })

    var ownedFilesSet = new util.HashSet[String]()
    var ownedFiles = new File(this.filePath).listFiles()
    for (ownedFile <- ownedFiles) {
      ownedFilesSet.add(ownedFile.getName)
    }
    var filesItr = asScalaIterator(files.iterator())
    filesItr.filter(x => {
      !ownedFilesSet.contains(x.name + FileSystemConstants.SNAPPY_EXTENSION)
    }).foreach(x => {
      fileToBeDownloaded.put(x.name, x)
      remoteFilesSet.add(x.name)
      for (i <- x.nodeIds) {
        nodeToFileMap.get(i).add(x)
      }
    })
  }




  def downloadFilesInBatch(): Unit = {
    val nodeList = new util.ArrayList[Integer](nodeToFileMap.keySet());
    Collections.shuffle(nodeList)
    val scalaItr = asScalaIterator(nodeList.iterator())
    while (scalaItr.hasNext) {
      val nodeId = scalaItr.next()
      val serNode = nodeInfo.get(nodeId)
      var nodeFiles = nodeToFileMap.get(nodeId)
      if (!nodeFiles.isEmpty) {
        if (!blackListedIps.contains(serNode.getIp)) {
          HHRDDIteratorHelper.downloadNodeFiles(this.tmpDownloadPath, this.filePath, nodeFiles, serNode.getIp, serNode.getPort, fileToBeDownloaded)
        }
      }
      scalaItr.remove()
    }
  }


  def readBlackListedIps = {
    if (blackListDir.exists) {
      val tempIpFiles = blackListDir.listFiles
      val ip = new Array[String](tempIpFiles.length)
      var index = 0
      while (index < ip.length) {
        blackListedIps.add(tempIpFiles(index).getName)
        index += 1
      }
    }
  }

  markRemoteFiles
  readBlackListedIps
  downloadFilesInBatch
  downloadFilesIndivisually
  logger.info("Total Files:{} Remote Files:{}",files.size(),remoteFilesSet.size())


  private def downloadFilesIndivisually = {
    import scala.collection.JavaConversions._
    for (fileDetail <- fileToBeDownloaded.values()) {
      download(fileDetail)
    }
  }

  private def download(fileDetail: FileDetail) = {
    val file = new File(this.filePath + fileDetail.name + FileSystemConstants.SNAPPY_EXTENSION)
    var isFileDownloaded = false
    breakable {
      for (hostIndex <- fileDetail.nodeIds.indices) {
        val index = fileDetail.nodeIds(hostIndex)
        val ip = nodeInfo.get(index).getIp
        if (blackListedIps.contains(ip)) {
          if (blackListedIps.size == noOfReplicas) {
            logger.error("Failed to download" + file.getCanonicalPath)
            HHRDDIteratorHelper.deleteAllDownloadedFiles(remoteFilesSet, tmpDownloadPath, tmpDownloadDir)
            throw new RuntimeException("Application cannot run as nodes :: " + blackListedIps + " are not listening")
          }
        } else {
          val port = nodeInfo.get(index).getPort
          var maxRetry = 5
          while (!isFileDownloaded && maxRetry > 0) {
            isFileDownloaded = HHRDDIteratorHelper.downloadFile(this.tmpDownloadPath + fileDetail.name + FileSystemConstants.SNAPPY_EXTENSION, this.filePath + fileDetail.name + FileSystemConstants.ZIP_EXTENSION, ip, port)
            maxRetry -= 1
          }
          if (isFileDownloaded) {
            break
          }
          else {
            logger.info("Downloading failed for file {}/{} from nodes {} ", filePath, fileDetail.name, fileDetail.nodeIds)
            logger.info(" Node {} is dead", ip)
            HHRDDIteratorHelper.createAndAddBlackListIPFile(blackListDir, ip, blackListedIps)
          }
        }
      }
    }
  }

  private var blockItr: Iterator[BlockStatistics] = _
  private var currBlockStatistics: BlockStatistics = _
  private var currentDataFileSize: Long = _
  private var snappyInputStream: SnappyInputStream = _
  private var dataInputStream: BufferedInputStream = _
  private var fileInputStream: FileInputStream = _
  private var currentFile: String = _
  private var currentFilePath: String = _
  private val fileIterator = files.iterator
  private val byteBufferBytes = new Array[Byte](recordLength)
  private val byteBuffer = ByteBuffer.wrap(byteBufferBytes)
  private val hhRDDRowReader = new HHRDDRowReader(dataDescription, byteBuffer)
  private var unreadFlag = true
  private var blockStatisticsList: util.List[BlockStatistics] = _
  private val data = new Array[Any](colMap.length)
  private var readLen = 0L


  iterateOnFiles


  /**
    * Iterate on files.
    *
    * @throws IOException Signals that an I/O exception has occurred.
    */
  @throws[IOException]
  private def iterateOnFiles: Unit = {
    if (blockItr != null && blockItr.hasNext) {
      assignValidDataStream
      return
    }
    while (fileIterator.hasNext) {
      val fileDetail = fileIterator.next
      currentFile = fileDetail.name
      if (remoteFilesSet.contains(currentFile)) {
        currentFilePath = this.tmpDownloadPath + currentFile + FileSystemConstants.SNAPPY_EXTENSION
      }
      else {
        currentFilePath = this.filePath + currentFile + FileSystemConstants.SNAPPY_EXTENSION
      }
      blockStatisticsList = HHRDDIteratorHelper.readBlockStatisticsList(blockStatisticsFolderPath + fileDetail.blockStatisticsLocation + File.separator + currentFile)
      blockItr = HHBlockFilterUtility.getFilteredBlocks(filters, blockStatisticsList,serializableComparators,columnNameToIdxMap)
      if (blockItr.hasNext) {
        if (unreadFlag) {
          unreadFlag = false
        }
        assignValidDataStream
        return
      }
    }
  }

  private def assignValidDataStream: Unit = {
    currBlockStatistics = blockItr.next
    currentDataFileSize = currBlockStatistics.getDataSize
    setDataInputStream
  }

  def setDataInputStream: Unit = {
    fileInputStream = new FileInputStream(currentFilePath)
    snappyInputStream = new SnappyInputStream(fileInputStream)
    dataInputStream = new BufferedInputStream(snappyInputStream, currentDataFileSize.asInstanceOf[Int])
    readLen = currBlockStatistics.getStartPos
    dataInputStream.skip(currBlockStatistics.getStartPos)
  }

  override def hasNext: Boolean = {
    if (currentDataFileSize > 0) {
      return true
    }
    try {
      if (blockItr.hasNext) {
        readLen += currBlockStatistics.getDataSize
        currBlockStatistics = blockItr.next
        currentDataFileSize = currBlockStatistics.getDataSize
        dataInputStream.skip(currBlockStatistics.getStartPos - readLen)
        readLen = currBlockStatistics.getStartPos
        return true
      } else {
        closeDataInputStream
        iterateOnFiles
      }
    }
    catch {
      case exception: IOException =>
        HHRDDIteratorHelper.deleteAllDownloadedFiles(remoteFilesSet, tmpDownloadPath, tmpDownloadDir)
        throw new RuntimeException(exception)
    }
    if (currentDataFileSize > 0) {
      return true
    }
    if (unreadFlag) {
      unreadFlag = false
      currBlockStatistics = blockStatisticsList.get(0)
      currentDataFileSize = recordLength
      setDataInputStream
      return true
    }
    HHRDDIteratorHelper.deleteAllDownloadedFiles(remoteFilesSet, tmpDownloadPath, tmpDownloadDir)
    false
  }

  override def next: Row = {
    try {
      dataInputStream.read(byteBufferBytes)
      currentDataFileSize = currentDataFileSize - recordLength
    } catch {
      case e: IOException =>
        HHRDDIteratorHelper.deleteAllDownloadedFiles(remoteFilesSet, tmpDownloadPath, tmpDownloadDir)
        e.printStackTrace()
    }
    for (colIdx <- colMap.indices) {
      data(colIdx) = hhRDDRowReader.readAtColumn(colMap(colIdx))
    }
    Row.fromSeq(data)
  }

  @throws[IOException]
  private def closeDataInputStream: Unit = {
    if (snappyInputStream != null) {
      dataInputStream.close()
      snappyInputStream.close()
      fileInputStream.close()
    }
  }
}
