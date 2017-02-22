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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;

import scala.Tuple2;

/**
 * This class provides the iteration over the partition of binary file storage system.
 * 
 * @author pooshans
 *
 */
public class HHBinaryRDDIterator extends HHRDDIterator<byte[]> implements Serializable {

  private static final long serialVersionUID = 6639856882717975103L;
  private BufferedInputStream dataInputStream;
  private long currentDataFileSize;

  public HHBinaryRDDIterator(String filePath, int rowSize, List<Tuple2<String, int[]>> files,
      Map<Integer, SerializedNode> nodeInfo, String dataDirectory) throws IOException {
    super(filePath, rowSize, files, nodeInfo,dataDirectory);
  }

  @Override
  protected void readyFileProcess() throws IOException {
    if (hasNextFile()) {
      Tuple2<String, int[]> tuple2 = nextFile();
      currentFile = tuple2._1;
      currentFilePath = this.filePath + currentFile;
      dataInputStream = new BufferedInputStream(new FileInputStream(currentFilePath), 2097152);
      this.currentDataFileSize = dataInputStream.available();
    }
  }

  @Override
  protected boolean downloadFile(String filePath, String ip, int port) {
    Socket socket = null;
    try {
      File file = new File(filePath);
      int bufferSIze = 2048;
      SocketAddress socketAddress = new InetSocketAddress(ip, port);
      socket = new Socket();
      socket.connect(socketAddress, 1000);
      //socket = new Socket(ip, 8789); // need to remove hard coded port number
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
        closeStream();
        readyFileProcess();
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

  @Override
  protected void closeStream() throws IOException {
    if (dataInputStream != null) {
      dataInputStream.close();
      if (trackRemoteFiles.contains(currentFile)) {
        new File(currentFilePath).delete();
      }
    }
  }


}
