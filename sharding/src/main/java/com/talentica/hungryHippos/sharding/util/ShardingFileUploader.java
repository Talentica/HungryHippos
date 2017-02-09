package com.talentica.hungryHippos.sharding.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;

public class ShardingFileUploader implements Runnable {

  private Node node;
  private boolean success;
  private String destinationPath;
  private String sourceFile;
  private int idealSizeOfBuffer = 8192;

  public ShardingFileUploader(Node node, String sourceFile, String destinationPath) {
    this.node = node;
    this.sourceFile = sourceFile;
    this.destinationPath = destinationPath;
  }

  public boolean isSuccess() {
    return this.success;
  }

  public Node getNode() {
    return this.node;
  }

  @Override
  public void run() {
    DataInputStream dis = null;
    DataOutputStream dos = null;
    FileInputStream fis = null;
    Socket socket = null;

    try {
      socket = ServerUtils.connectToServer(node.getIp() + ":" + 8789, 50);
      dis = new DataInputStream(socket.getInputStream());
      dos = new DataOutputStream(socket.getOutputStream());
      long size = Files.size(Paths.get(sourceFile));
      dos.writeInt(HungryHippoServicesConstants.ACCEPT_FILE);
      dos.writeUTF(destinationPath);
      dos.writeLong(size);

      byte[] buffer = new byte[idealSizeOfBuffer];
      int read = 0;
      fis = new FileInputStream(new File(sourceFile));
      while ((read = fis.read(buffer)) != -1) {
        dos.write(buffer, 0, read);
      }
      dos.flush();
      this.success = dis.readBoolean();
      if (!this.success) {
        // TODO provide retry.
        System.out.println("transfer of shardingTable failed");
      } else {
        System.out.println("transfer completed");
      }

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      try {
        if (socket != null) {
          socket.close();
        }

        if (fis != null) {
          fis.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }


    }
  }
}


