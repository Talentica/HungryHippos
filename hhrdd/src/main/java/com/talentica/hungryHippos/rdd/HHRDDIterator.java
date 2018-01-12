/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
/**
 *
 */
package com.talentica.hungryHippos.rdd;

import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.apache.spark.executor.InputMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.AbstractIterator;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.*;

/**
 * The Class HHRDDIterator.
 *
 * @author pooshans
 */
public class HHRDDIterator extends AbstractIterator<byte[]> {

  /** The logger. */
  private static Logger logger = LoggerFactory.getLogger(HHRDDIterator.class);
  
  /** The byte buffer bytes. */
  // private ByteBuffer byteBuffer = null;
  private byte[] byteBufferBytes;
  
  /** The current data file size. */
  private long currentDataFileSize;
  
  /** The current file. */
  private String currentFile;
  
  /** The current file path. */
  private String currentFilePath;
  
  /** The data input stream. */
  private BufferedInputStream dataInputStream;
  
  /** The record length. */
  // private HHRDDRowReader hhRDDRowReader;
  private int recordLength;
  
  /** The remote files. */
  private Set<String> remoteFiles;
  
  /** The file path. */
  private String filePath;
  
  /** The file iterator. */
  private Iterator<Tuple2<String,int[]>> fileIterator;
  
  /** The black listed ips. */
  private Set<String> blackListedIps;

  private File tmpDownloadDir;

  private String tmpDownloadPath;

  private InputMetrics inputMetrics;

  /**
   * Instantiates a new HHRDD iterator.
   *
   * @param filePath the file path
   * @param rowSize the row size
   * @param files the files
   * @param nodeInfo the node info
   * @param tmpDir the tmp dir
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public HHRDDIterator(String filePath, int rowSize, List<Tuple2<String,int[]>> files, Map<Integer, SerializedNode> nodeInfo,File tmpDir, InputMetrics inputMetrics) throws IOException {
    this.inputMetrics = inputMetrics;
    this.filePath = filePath+File.separator;
    remoteFiles = new HashSet<>();
    blackListedIps = new HashSet<>();
    tmpDownloadDir = HHRDD.createTempDir();
    tmpDownloadPath = tmpDownloadDir.getAbsolutePath()+File.separator;
      if(tmpDir.exists()){
        File[] tempIpfiles = tmpDir.listFiles();
        String[] ip = new String[tempIpfiles.length];
        for (int index = 0; index < ip.length; index++) {
          blackListedIps.add(tempIpfiles[index].getName());
        }
      }
    for(Tuple2<String,int[]> tuple2: files){
      File file = new File(this.filePath+tuple2._1);
      if (!file.exists()) {
      boolean isFileDownloaded = false;
      for (int hostIndex = 0; hostIndex < tuple2._2.length; hostIndex++) {
        int index = tuple2._2[hostIndex];
        String ip = nodeInfo.get(index).getIp();
          if (blackListedIps.contains(ip)) {
              if(blackListedIps.size()==tuple2._2.length) {
                deleteAllDownloadedFiles();
                throw new RuntimeException(
                        "Application cannot run as nodes :: " + blackListedIps + " are not listening");
              }
              continue;
          }
        int port = nodeInfo.get(index).getPort();
        int maxRetry = 5;
        while (!isFileDownloaded && (maxRetry--) > 0) {
          isFileDownloaded = downloadFile(this.tmpDownloadPath + tuple2._1,this.filePath+tuple2._1, ip, port);
        }
        if (isFileDownloaded) {
          break;
        } else {
          logger.info("Downloading failed for file {}/{} from nodes {} ", filePath, tuple2._1, tuple2._2);
          logger.info(" Node {} is dead", ip);
          createAndAddBlackListIPFile(tmpDir, ip);
        }
      }
      remoteFiles.add(tuple2._1);
      }
    }
    fileIterator = files.iterator();
    iterateOnFiles();


    // this.hhRDDRowReader = new HHRDDRowReader(dataDescription);
    this.byteBufferBytes = new byte[rowSize];
    // this.byteBuffer = ByteBuffer.wrap(byteBufferBytes);
    this.recordLength = rowSize;
    // this.hhRDDRowReader.setByteBuffer(byteBuffer);
  }

  /**
   * Creates the and add black list IP file.
   *
   * @param tmpDir the tmp dir
   * @param ip the ip
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void createAndAddBlackListIPFile(File tmpDir, String ip) throws IOException {
    File blacklistIPFile = new File(tmpDir.getAbsolutePath() + File.separator + ip);
    if (!blacklistIPFile.exists()) {
      blacklistIPFile.createNewFile();
    }
    blackListedIps.add(ip);
  }

  /**
   * Iterate on files.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void iterateOnFiles() throws IOException {
    while(fileIterator.hasNext()){
      Tuple2<String,int[]> tuple2 = fileIterator.next();
      currentFile = tuple2._1;
      if(remoteFiles.contains(currentFile)){
        currentFilePath = this.tmpDownloadPath+currentFile;
      }else {
        currentFilePath = this.filePath + currentFile;
      }
      File currentFileObj = new File(currentFilePath);
      this.currentDataFileSize = currentFileObj.length();
      if(currentDataFileSize==0) continue;
      this.dataInputStream = new BufferedInputStream(new FileInputStream(currentFilePath), 2097152);
      return;
    }
  }

  /**
   * Download file.
   *
   * @param downloadFilePath the download file path
   * @param filePath the file path
   * @param ip the ip
   * @param port the port
   * @return true, if successful
   */
  private boolean downloadFile(String downloadFilePath,String filePath, String ip,int port) {
    Socket socket = null;
    try {
      File file = new File(downloadFilePath);
      file.createNewFile();
      int bufferSize = 2048;
      SocketAddress socketAddress = new InetSocketAddress(ip, port);
      socket = new Socket();
      socket.connect(socketAddress, 10000);
      DataInputStream dis = new DataInputStream(socket.getInputStream());
      DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
      dos.writeInt(HungryHippoServicesConstants.FILE_PROVIDER);
      dos.writeUTF(filePath);
      dos.flush();
      long fileSize = dis.readLong();
      byte[] buffer = new byte[bufferSize];
      FileOutputStream fos = new FileOutputStream(file);
      BufferedOutputStream bos =
          new BufferedOutputStream(fos, bufferSize * 10);
      int len;
      while (fileSize > 0) {
        len = dis.read(buffer);
        bos.write(buffer, 0, len);
        fileSize -= len;
      }
      bos.flush();
      fos.flush();
      bos.close();
      fos.close();
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

  }

  /* (non-Javadoc)
   * @see scala.collection.Iterator#hasNext()
   */
  @Override
  public boolean hasNext() {
    try {
      if (currentDataFileSize <= 0) {
        closeDataInputStream();
        iterateOnFiles();
      }
    } catch (IOException exception) {
      this.deleteAllDownloadedFiles();
      throw new RuntimeException(exception);
    }
    boolean hasData = currentDataFileSize > 0;
    if(!hasData){
      this.deleteAllDownloadedFiles();
    }
    return hasData;

  }

  /* (non-Javadoc)
   * @see scala.collection.Iterator#next()
   */
  @Override
  public byte[] next() {
    try {
      dataInputStream.read(byteBufferBytes);
      inputMetrics.incBytesRead(recordLength);
      inputMetrics.incRecordsRead(1);
      currentDataFileSize = currentDataFileSize - recordLength;
    } catch (IOException e) {
      this.deleteAllDownloadedFiles();
      e.printStackTrace();
    }
    return byteBufferBytes;
  }

  /**
   * Close dats input stream.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void closeDataInputStream() throws IOException {
    if (dataInputStream != null) {
      dataInputStream.close();
      if(remoteFiles.contains(currentFile)){
        new File(currentFilePath).delete();
      }
      remoteFiles.remove(currentFile);
    }
  }

    /**
     * Delete all downloaded files.
     */
    private void deleteAllDownloadedFiles(){
        for(String remoteFileName:remoteFiles){
            File remoteFile = new File(this.tmpDownloadPath+remoteFileName);
            remoteFile.delete();
        }

      tmpDownloadDir.delete();
    }

}
