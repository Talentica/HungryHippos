package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.node.service.DataAppenderService;
import com.talentica.hungryHippos.node.service.DataDistributorService;
import com.talentica.hungryHippos.node.service.SCPAccessService;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

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
        case HungryHippoServicesConstants.DATA_DISTRIBUTOR:
          DataDistributorStarter.dataDistributorService.execute(new DataDistributorService(socket));
          break;
        case HungryHippoServicesConstants.FILE_PROVIDER:
          DataDistributorStarter.fileProviderService.execute(new FileProviderService(socket));
          break;
        case HungryHippoServicesConstants.DATA_APPENDER:
          DataDistributorStarter.dataAppenderServices.execute(new DataAppenderService(socket));
          break;
        case HungryHippoServicesConstants.SCP_ACCESS:
          DataDistributorStarter.scpAccessServices.execute(new SCPAccessService(socket));
          break;
        default:
          socket.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }



}
