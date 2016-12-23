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
import java.io.Serializable;
import java.net.Socket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.AbstractIterator;

/**
 * @author pooshans
 *
 */
public class HHRDDIterator extends AbstractIterator<byte[]> implements Serializable {

  private static final long serialVersionUID = 6639856882717975103L;
  private static Logger logger = LoggerFactory.getLogger(HHRDDIterator.class);
  // private ByteBuffer byteBuffer = null;
  private byte[] byteBufferBytes;
  private long currentDataFileSize;
  private BufferedInputStream dataInputStream;
  // private HHRDDRowReader hhRDDRowReader;
  private int recordLength;

  public HHRDDIterator(String filePath, int rowSize,List<String> ipList) throws IOException {
    File file = new File(filePath);
    if (!file.exists()) {
      logger.info("Downloading file {} from hosts {} ",filePath,ipList);
      boolean fileDownloaded = false;
      while (!fileDownloaded) {
        for (String ip : ipList) {
          fileDownloaded = downloadFile(filePath, ip);
          logger.info("File downloaded success status {} from ip {}",fileDownloaded,ip);
          if (fileDownloaded) {
            break;
          }
        }
      }

    }
    this.dataInputStream = new BufferedInputStream(new FileInputStream(filePath), 2097152);
    this.currentDataFileSize = dataInputStream.available();
    // this.hhRDDRowReader = new HHRDDRowReader(dataDescription);
    this.byteBufferBytes = new byte[rowSize];
    // this.byteBuffer = ByteBuffer.wrap(byteBufferBytes);
    this.recordLength = rowSize;
    // this.hhRDDRowReader.setByteBuffer(byteBuffer);
  }

  private boolean downloadFile(String filePath, String ip) {
    Socket socket = null;
    try {
      File file = new File(filePath);
      int port = 8789;
      int bufferSIze = 2048;
      socket = new Socket(ip, port);
      DataInputStream dis = new DataInputStream(socket.getInputStream());
      DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
      dos.writeInt(2);
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
      String status = dis.readUTF();
      if (!"SUCCESS".equals(status)) {
        return false;
      }
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
    }
  }

}
