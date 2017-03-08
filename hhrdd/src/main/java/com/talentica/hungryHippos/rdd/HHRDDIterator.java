/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.AbstractIterator;

import java.io.*;
import java.net.Socket;
import java.util.*;

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
  public HHRDDIterator(String filePath, int rowSize, List<Tuple2<String,int[]>> files, Map<Integer, String> nodIdToIp) throws IOException {
    this.filePath = filePath+File.separator;
    remoteFiles = new HashSet<>();
    for(Tuple2<String,int[]> tuple2: files){
      File file = new File(filePath+File.separator+tuple2._1);
      if (!file.exists()) {
        logger.info("Downloading file {}/{} from nodes {} ",filePath,tuple2._1,tuple2._2);
        boolean isFileDownloaded = false;
        while (!isFileDownloaded) {
          for (int id : tuple2._2) {
            String ip= nodIdToIp.get(id);
            isFileDownloaded = downloadFile(this.filePath+tuple2._1, ip);
            logger.info("File downloaded success status {} from ip {}",isFileDownloaded,ip);
            if (isFileDownloaded) {
              break;
            }
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

  private void iterateOnFiles() throws IOException {
    if(fileIterator.hasNext()){
      Tuple2<String,int[]> tuple2 = fileIterator.next();
      currentFile = tuple2._1;
      currentFilePath = this.filePath+currentFile;
      this.dataInputStream = new BufferedInputStream(new FileInputStream(currentFilePath), 2097152);
      this.currentDataFileSize = dataInputStream.available();
    }
  }

  private boolean downloadFile(String filePath, String ip) {
    Socket socket = null;
    try {
      File file = new File(filePath);
      int port = 2324;
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
