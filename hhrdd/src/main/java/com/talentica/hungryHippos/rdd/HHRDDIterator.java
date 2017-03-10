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
import java.net.Socket;
import java.util.Arrays;
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
 * @author pooshans
 *
 */
public class HHRDDIterator extends AbstractIterator<byte[]> {

  private static Logger logger = LoggerFactory.getLogger(HHRDDIterator.class);
  // private ByteBuffer byteBuffer = null;
  private byte[] byteBufferBytes;
  private long currentDataFileSize;
  private String currentFile;
  private String currentFilePath;
  private BufferedInputStream dataInputStream;
  // private HHRDDRowReader hhRDDRowReader;
  private int recordLength;
  private Set<String> remoteFiles;
  private String filePath;
  private Iterator<Tuple2<String,int[]>> fileIterator;
  public HHRDDIterator(String filePath, int rowSize, List<Tuple2<String,int[]>> files, Map<Integer, SerializedNode> nodeInfo,File tmpDir) throws IOException {
    this.filePath = filePath+File.separator;
    remoteFiles = new HashSet<>();
    for(Tuple2<String,int[]> tuple2: files){
      logger.info("Downloading file {}/{} from nodes {} ", filePath, tuple2._1, tuple2._2);
      boolean isFileDownloaded = false;
      for (int hostIndex = 0; hostIndex < tuple2._2.length; hostIndex++) {
        int index = tuple2._2[hostIndex];
        String ip = nodeInfo.get(index).getIp();
        int port = nodeInfo.get(index).getPort();

        File blacklistIPFile = new File(tmpDir + File.separator + ip);
        if (blacklistIPFile.exists()) {
          continue;
        }

        int maxRetry = 5;
        while (!isFileDownloaded && (maxRetry--) > 0) {
          isFileDownloaded = downloadFile(this.filePath + tuple2._1, ip, port);
        }
        if (isFileDownloaded) {
          logger.info("File downloaded success status {} from ip {}", isFileDownloaded, ip);
          break;
        } else {
          logger.info(" Node {} is dead", ip);
          if (!blacklistIPFile.exists()) {
            blacklistIPFile.createNewFile();
          }
        }
      }
      if (!isFileDownloaded) {
        File[] tempIpfiles = tmpDir.listFiles();
        String[] ip = new String[tempIpfiles.length];
        for (int index = 0; index < ip.length; index++) {
          ip[index] = tempIpfiles[index].getName();
        }
        throw new RuntimeException(
            "Application cannot run as nodes :: " + Arrays.toString(ip) + " are not listening");
      }
      remoteFiles.add(tuple2._1);}
    fileIterator = files.iterator();
    iterateOnFiles();


    // this.hhRDDRowReader = new HHRDDRowReader(dataDescription);
    this.byteBufferBytes = new byte[rowSize];
    // this.byteBuffer = ByteBuffer.wrap(byteBufferBytes);
    this.recordLength = rowSize;
    // this.hhRDDRowReader.setByteBuffer(byteBuffer);
  }

  private void iterateOnFiles() throws IOException {
    if(fileIterator.hasNext()){
      Tuple2<String,int[]> tuple2 = fileIterator.next();
      currentFile = tuple2._1;
      currentFilePath = this.filePath+currentFile;
      this.dataInputStream = new BufferedInputStream(new FileInputStream(currentFilePath), 2097152);
      this.currentDataFileSize = dataInputStream.available();
    }
  }

  private boolean downloadFile(String filePath, String ip,int port) {
    Socket socket = null;
    try {
      File file = new File(filePath);
      int bufferSIze = 2048;
      socket = new Socket(ip, port);
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

  private void closeDatsInputStream() throws IOException {
    if (dataInputStream != null) {
      dataInputStream.close();
      if(remoteFiles.contains(currentFile)){
        new File(currentFilePath).delete();
      }
    }
  }

}
