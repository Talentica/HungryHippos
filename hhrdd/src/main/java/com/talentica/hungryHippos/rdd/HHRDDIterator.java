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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;

import scala.Tuple2;
import scala.collection.AbstractIterator;

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
  public HHRDDIterator(String filePath, int rowSize, List<Tuple2<String,int[]>> files, Map<Integer, SerializedNode> nodeInfo,File tmpDir) throws IOException {
    this.filePath = filePath+File.separator;
    remoteFiles = new HashSet<>();
    blackListedIps = new HashSet<>();
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
      logger.info("Downloading file {}/{} from nodes {} ", filePath, tuple2._1, tuple2._2);
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
          isFileDownloaded = downloadFile(this.filePath + tuple2._1, ip, port);
        }
        if (isFileDownloaded) {
          logger.info("File downloaded success status {} from ip {}", isFileDownloaded, ip);
          break;
        } else {
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
    if(fileIterator.hasNext()){
      Tuple2<String,int[]> tuple2 = fileIterator.next();
      currentFile = tuple2._1;
      currentFilePath = this.filePath+currentFile;
      this.dataInputStream = new BufferedInputStream(new FileInputStream(currentFilePath), 2097152);
      this.currentDataFileSize = dataInputStream.available();
    }
  }

  /**
   * Download file.
   *
   * @param filePath the file path
   * @param ip the ip
   * @param port the port
   * @return true, if successful
   */
  private boolean downloadFile(String filePath, String ip,int port) {
    Socket socket = null;
    try {
      File file = new File(filePath);
      int bufferSIze = 2048;
      SocketAddress socketAddress = new InetSocketAddress(ip, port);
      socket = new Socket();
      socket.connect(socketAddress, 10000);
      DataInputStream dis = new DataInputStream(socket.getInputStream());
      DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
      dos.writeInt(HungryHippoServicesConstants.FILE_PROVIDER);
      dos.writeUTF(filePath);
      dos.flush();
      long fileSize = dis.readLong();
      byte[] buffer = new byte[bufferSIze];
      BufferedOutputStream bos =
          new BufferedOutputStream(new FileOutputStream(file), bufferSIze * 10);
      int len;
      while (fileSize > 0) {
        len = dis.read(buffer);
        bos.write(buffer, 0, len);
        fileSize -= len;
      }
      bos.flush();
      bos.close();
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
        closeDatsInputStream();
        iterateOnFiles();
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
    return currentDataFileSize > 0;

  }

  /* (non-Javadoc)
   * @see scala.collection.Iterator#next()
   */
  @Override
  public byte[] next() {
    try {
      dataInputStream.read(byteBufferBytes);
      currentDataFileSize = currentDataFileSize - recordLength;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return byteBufferBytes;
  }

  /**
   * Close dats input stream.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void closeDatsInputStream() throws IOException {
    if (dataInputStream != null) {
      dataInputStream.close();
      if(remoteFiles.contains(currentFile)){
        new File(currentFilePath).delete();
      }
    }
  }

    /**
     * Delete all downloaded files.
     */
    private void deleteAllDownloadedFiles(){
        for(String remoteFileName:remoteFiles){
            File remoteFile = new File(filePath+remoteFileName);
            remoteFile.delete();
        }
    }

}
