package com.talentica.hungryHippos.node.service;

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

public class AcceptFileService implements Runnable {

  private Socket socket;

  public AcceptFileService(Socket socket) {
    this.socket = socket;
  }

  @Override
  public void run() {
    DataInputStream dis = null;
    DataOutputStream dos = null;
    try {
      dis = new DataInputStream(this.socket.getInputStream());
      dos = new DataOutputStream(this.socket.getOutputStream());
      String destinationPath = dis.readUTF();
      long size = dis.readLong();

      int idealBufSize = 8192;
      byte[] buffer = new byte[idealBufSize];
      File file = new File(destinationPath);
      Path parentDir = Paths.get(file.getParent());
      if (!(Files.exists(parentDir))) {
        Files.createDirectories(parentDir);
      }

      OutputStream bos = new BufferedOutputStream(new FileOutputStream(destinationPath));
      int read = 0;
      while (size != 0) {
        read = dis.read(buffer);
        bos.write(buffer, 0, read);
        size -= read;
      }
      bos.flush();
      bos.close();
      dos.writeBoolean(true);
      dos.flush();
    } catch (IOException e) {
      try {
        dos.writeBoolean(false);
        dos.flush();
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }

      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (this.socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }


  }

}
