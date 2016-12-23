package com.talentica.hungryHippos.node;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

import com.talentica.hungryHippos.node.datadistributor.DataDistributorService;

public class ServiceDeligator implements Runnable {

  private Socket socket;

  public ServiceDeligator(Socket socket) throws IOException {
    this.socket = socket;
  }

  @Override
  public void run() {
    try {
      DataInputStream dis = new DataInputStream(this.socket.getInputStream());
      int serviceId = dis.readInt();
      switch (serviceId) {
        case 1:
          DataDistributorStarter.dataDistributorService.execute(new DataDistributorService(socket));
          break;
        case 2:
          DataDistributorStarter.fileProviderService.execute(new FileProviderService(socket));
          break;
        default:
          this.socket.close();
          break;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }



}
