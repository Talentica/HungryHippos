/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;

import scala.Tuple2;

/**
 * This class provides the iterator over the partition for the text storage system.
 * 
 * @author pooshans
 *
 */
public class HHTextRDDIterator extends HHRDDIterator<String> implements Serializable {

  private static final long serialVersionUID = 3316311904434033364L;
  private BufferedReader bufferedReader;

  public HHTextRDDIterator(String filePath, List<Tuple2<String, int[]>> files,
      Map<Integer, SerializedNode> nodeInfo) throws IOException {
    super(filePath, files, nodeInfo);
  }


  @Override
  protected boolean downloadFile(String filePath, String ip, int port) {
    Socket socket = null;
    try {
      File file = new File(filePath);
      int bufferSIze = 2048;
      socket = new Socket(ip, port);
      InputStreamReader isr = new InputStreamReader(socket.getInputStream());
      BufferedReader br = new BufferedReader(isr);
      DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
      dos.writeInt(HungryHippoServicesConstants.FILE_PROVIDER);
      dos.writeUTF(filePath);
      dos.flush();
      BufferedWriter bos = new BufferedWriter(new FileWriter(file), bufferSIze * 10);
      String line;
      while (br.ready()) {
        line = br.readLine();
        bos.write(line);
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
      boolean isReady = bufferedReader.ready();
      if (!isReady) {
        closeStream();
        readyFileProcess();
      }
      return isReady;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public String next() {
    try {
      return bufferedReader.readLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  protected void readyFileProcess() throws IOException {
    if (fileIterator.hasNext()) {
      Tuple2<String, int[]> tuple2 = fileIterator.next();
      currentFile = tuple2._1;
      currentFilePath = this.filePath + currentFile;
      this.bufferedReader = new BufferedReader(new FileReader(currentFilePath), 2097152);
    }
  }

  @Override
  protected void closeStream() throws IOException {
    if (bufferedReader != null) {
      bufferedReader.close();
      if (trackRemoteFiles.contains(currentFile)) {
        new File(currentFilePath).delete();
      }
    }
  }

}
