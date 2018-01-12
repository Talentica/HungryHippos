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
import java.net.{InetSocketAddress, Socket}
import java.nio.file._
import java.util

import com.talentica.hungryHippos.utility.{FileSystemConstants, HungryHippoServicesConstants}
import com.talentica.hungryhippos.filesystem.BlockStatistics
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by rajkishoreh.
  */
object HHRDDIteratorHelper {

  /** The temp dir attempts. */
  private val TEMP_DIR_ATTEMPTS = 10000

  private val logger = LoggerFactory.getLogger(classOf[HHRDDIterator])
  private val zipFileSystemEnv = new util.HashMap[String, String]
  zipFileSystemEnv.put("create", "true")

  /**
    * Download file.
    *
    * @param downloadFilePath the download file path
    * @param filePath         the file path
    * @param ip               the ip
    * @param port             the port
    * @return true, if successful
    */
  def downloadFile(downloadFilePath: String, filePath: String, ip: String, port: Int) = {

    var socket: Socket = null
    try {
      socket = new Socket
      val file = new File(downloadFilePath)
      file.createNewFile
      val bufferSize = 2048
      val socketAddress = new InetSocketAddress(ip, port)
      socket.connect(socketAddress, 10000)
      val dis = new DataInputStream(socket.getInputStream)
      val dos = new DataOutputStream(socket.getOutputStream)
      dos.writeInt(HungryHippoServicesConstants.FILE_PROVIDER)
      dos.writeBoolean(true)
      dos.writeUTF(filePath)
      dos.flush()
      var fileSize = dis.readLong
      val buffer = new Array[Byte](bufferSize)
      val fos = new FileOutputStream(file)
      val bos = new BufferedOutputStream(fos, bufferSize * 10)
      var len = 0
      while (fileSize > 0) {
        len = dis.read(buffer)
        bos.write(buffer, 0, len)
        fileSize -= len
      }
      bos.flush()
      fos.flush()
      bos.close()
      fos.close()
      dos.writeBoolean(false)
      dos.flush()
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    } finally if (socket != null) try
      socket.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }


  def downloadNodeFiles(downloadFolderPath: String, folderPath: String, nodeFiles: util.List[FileDetail], ip: String, port: Int, fileToBeDownloaded: util.HashMap[String,FileDetail]) = {

    var socket: Socket = null
    try {
      socket = new Socket
      val bufferSize = 2048
      val buffer = new Array[Byte](bufferSize)
      val socketAddress = new InetSocketAddress(ip, port)
      socket.connect(socketAddress, 10000)
      val dis = new DataInputStream(socket.getInputStream)
      val dos = new DataOutputStream(socket.getOutputStream)
      dos.writeInt(HungryHippoServicesConstants.FILE_PROVIDER)
      val scalaItr = asScalaIterator(nodeFiles.iterator())
      while (scalaItr.hasNext) {
        val nodeFile = scalaItr.next()
        if (fileToBeDownloaded.containsKey(nodeFile.name)) {
          dos.writeBoolean(true)
          dos.writeUTF(folderPath + nodeFile.name + FileSystemConstants.ZIP_EXTENSION)
          val file = new File(downloadFolderPath + nodeFile.name + FileSystemConstants.ZIP_EXTENSION)
          file.createNewFile()
          val fos = new FileOutputStream(file)
          val bos = new BufferedOutputStream(fos, bufferSize * 10)
          var len = 0
          dos.flush()
          var fileSize = dis.readLong
          while (fileSize > 0) {
            len = dis.read(buffer)
            bos.write(buffer, 0, len)
            fileSize -= len
          }
          bos.flush()
          fos.flush()
          bos.close()
          fos.close()
          fileToBeDownloaded.remove(nodeFile.name)
          file.deleteOnExit()
        }
      }
      dos.writeBoolean(false)
      dos.flush()
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    } finally if (socket != null) try
      socket.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  /**
    * Delete all downloaded files.
    */
  def deleteAllDownloadedFiles(remoteFilesToNodeIdMap: util.HashSet[String], tmpDownloadPath: String, tmpDownloadDir: File): Unit = {
    import scala.collection.JavaConversions._
    for (remoteFileName <- remoteFilesToNodeIdMap) {
      val remoteFile = new File(tmpDownloadPath + remoteFileName + FileSystemConstants.ZIP_EXTENSION)
      remoteFile.delete
    }
    FileUtils.deleteQuietly(tmpDownloadDir)

  }

  /**
    * Creates the and add black list IP file.
    *
    * @param tmpDir the tmp dir
    * @param ip     the ip
    * @throws IOException Signals that an I/O exception has occurred.
    */
  @throws[IOException]
  def createAndAddBlackListIPFile(tmpDir: File, ip: String, blackListedIps: util.HashSet[String]): Unit = {
    val blacklistIPFile = new File(tmpDir.getAbsolutePath + File.separator + ip)
    if (!blacklistIPFile.exists) blacklistIPFile.createNewFile
    blackListedIps.add(ip)
  }

  /**
    * Creates the temp dir.
    *
    * @return the file
    */
  def createTempDir: File = {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = System.currentTimeMillis + "-"
    var counter = 0
    for (counter <- 0 until TEMP_DIR_ATTEMPTS) {
      val tempDir = new File(baseDir, baseName + counter)
      if (tempDir.mkdir) {
        tempDir.deleteOnExit()
        return tempDir
      }
    }
    throw new IllegalStateException("Failed to create directory within " + TEMP_DIR_ATTEMPTS + " attempts (tried " + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')')
  }

  def readBlockStatisticsList(blockStatisticsFileLocation: String): util.List[BlockStatistics] = {

    var blockStatsList: util.List[BlockStatistics] = null
    var inputStream: InputStream = null
    var bufferedInputStream: BufferedInputStream = null
    try {
      inputStream = new FileInputStream(blockStatisticsFileLocation)
      bufferedInputStream = new BufferedInputStream(inputStream)
      var ois: ObjectInputStream = null
      try {
        ois = new ObjectInputStream(inputStream)
        blockStatsList = ois.readObject().asInstanceOf[util.List[BlockStatistics]];
      } finally {
        if (ois != null) {
          ois.close()
        }
      }
    } catch {
      case e: NoSuchFileException => {
        logger.error(blockStatisticsFileLocation + " " + e.toString)
        throw e;
      }
    } finally {
      if (bufferedInputStream != null) {
        bufferedInputStream.close()
        inputStream.close()
      }
    }

    blockStatsList
  }
}
