package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.apache.commons.io.FileUtils;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by rajkishoreh on 24/11/16.
 */
public class DataDistributorService implements Runnable {


  private DataInputStream dataInputStream;
  private DataOutputStream dataOutputStream;
  private Socket socket;

  public DataDistributorService(Socket socket) throws IOException {
    this.socket = socket;
    this.dataInputStream = new DataInputStream(socket.getInputStream());
    this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
  }

  @Override
  public void run() {
    String hhFilePath = null;
    String srcDataPath = null;

    try {
      hhFilePath = dataInputStream.readUTF();
      srcDataPath = dataInputStream.readUTF();

      byte[] buffer = new byte[socket.getReceiveBufferSize()];

      int read = 0;
      File file = new File(srcDataPath);
      Path parentDir = Paths.get(file.getParent());
      if (!(Files.exists(parentDir))) {
        Files.createDirectories(parentDir);
      }

      long size = dataInputStream.readLong();

      try (OutputStream bos =
          new BufferedOutputStream(new FileOutputStream(new File(srcDataPath)))) {

        while (size != 0) {
          read = dataInputStream.read(buffer);
          bos.write(buffer, 0, read);
          size -= read;
        }
        bos.flush();
      }

      dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
      System.out.println("finished reading");
      if (!NewDataHandler.checkIfFailed(hhFilePath)) {

        DataDistributor.distribute(hhFilePath, srcDataPath);
      }
      dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
      dataOutputStream.flush();
    } catch (Exception e) {
      if (hhFilePath != null) {
        NewDataHandler.updateFailure(hhFilePath, NodeInfo.INSTANCE.getIp() + " : " + e.toString());
      }
      try {
        dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
        dataOutputStream.flush();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      e.printStackTrace();
    } finally {

      if (srcDataPath != null) {
        try {
          FileUtils.deleteDirectory((new File(srcDataPath)).getParentFile());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      try {
        socket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
