/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.node.joiners.NodeFileJoiner;
import com.talentica.hungryHippos.node.joiners.TarFileJoiner;
import com.talentica.hungryHippos.node.joiners.UnTarStrategy;
import com.talentica.hungryHippos.node.service.*;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

public class ServiceDelegator implements Runnable {

  private Socket socket;

  public ServiceDelegator(Socket socket) throws IOException {
    this.socket = socket;
  }

  @Override
  public void run() {
    try {
      DataInputStream dis = new DataInputStream(this.socket.getInputStream());
      int serviceId = dis.readInt();
      switch (serviceId) {
        case HungryHippoServicesConstants.DATA_DISTRIBUTOR:
          DataDistributorStarter.dataDistributorService.execute(new PublishAccessService(socket));
          break;
        case HungryHippoServicesConstants.FILE_PROVIDER:
          DataDistributorStarter.fileProviderService.execute(new FileProviderService(socket));
          break;
        case HungryHippoServicesConstants.TAR_DATA_APPENDER:
          DataDistributorStarter.commonServicePoolCache
                  .execute(new DataAppenderService(socket,(x,y)->new TarFileJoiner(x,y,UnTarStrategy.UNTAR_ON_CONTINUOUS_STREAMS)));
          break;
        case HungryHippoServicesConstants.NODE_DATA_APPENDER:
          DataDistributorStarter.commonServicePoolCache.
                  execute(new DataAppenderService(socket,(x,y)-> new NodeFileJoiner(x,y)));
          break;
        case HungryHippoServicesConstants.METADATA_UPDATER:
          DataDistributorStarter.metadataUpdaterServices
              .execute(new MetaDataUpdaterService(socket));
          break;
        case HungryHippoServicesConstants.METADATA_SYNCHRONIZER:
          DataDistributorStarter.metadataSynchronizerServices
              .execute(new MetaDataSynchronizerService(socket));
          break;
        case HungryHippoServicesConstants.ACCEPT_FILE:
          DataDistributorStarter.fileService.execute(new AcceptFileService(socket));
          break;
        default:
          socket.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
      try {
        socket.close();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }

  }



}
