package com.talentica.hungryHippos.node;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;

public class FileProviderService implements Runnable {

  private Socket socket;

  public FileProviderService(Socket socket) throws IOException {
    this.socket = socket;
  }

  @Override
  public void run() {
    DataInputStream dis = null;
    DataOutputStream dos = null;
    try {
      dis = new DataInputStream(socket.getInputStream());
      dos = new DataOutputStream(socket.getOutputStream());
      int bufferSize = 2048;
      String filePath = dis.readUTF();
      File requestedFile = new File(filePath);
      long fileSize = requestedFile.length();
      dos.writeLong(fileSize);
      BufferedInputStream bis =
          new BufferedInputStream(new FileInputStream(requestedFile), bufferSize*10);
      byte[] buffer = new byte[bufferSize];
      int len;
      while ((len = bis.read(buffer)) > -1) {
        dos.write(buffer, 0, len);
      }
      bis.close();
      dos.flush();
    } catch (Exception e) {     
        e.printStackTrace();      
    }
    finally {
      try {
        this.socket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }


}
