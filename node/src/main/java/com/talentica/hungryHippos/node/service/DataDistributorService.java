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

import com.talentica.hungryHippos.node.datareceiver.HHFileStatusCoordinator;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.node.DataDistributorStarter;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryHippos.utility.MemoryStatus;

/**
 * Created by rajkishoreh on 24/11/16.
 */
public class DataDistributorService implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DataDistributorService.class);
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

      int idealBufSize = socket.getReceiveBufferSize();
      byte[] buffer;
      if (MemoryStatus.getUsableMemory() > idealBufSize) {
        buffer = new byte[socket.getReceiveBufferSize()];
      } else {
        buffer = new byte[2048];
      }

      int read = 0;
      File file = new File(srcDataPath);
      Path parentDir = Paths.get(file.getParent());
      if (!(Files.exists(parentDir))) {
        Files.createDirectories(parentDir);
      }

      long size = dataInputStream.readLong();
      FileOutputStream fos= new FileOutputStream(srcDataPath);
      OutputStream bos = new BufferedOutputStream(fos);

      while (size != 0) {
        read = dataInputStream.read(buffer);
        bos.write(buffer, 0, read);
        size -= read;
      }
      bos.flush();
      fos.flush();
      bos.close();
      fos.close();
      System.gc();
      dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
      dataOutputStream.flush();
      if (!HHFileStatusCoordinator.checkIfFailed(hhFilePath)) {
        DataDistributor.distribute(hhFilePath, srcDataPath);
      }
      logger.info("Data distributed successfully for {}",srcDataPath);
      dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
      dataOutputStream.flush();

    } catch (Exception e) {
      if (hhFilePath != null) {
        HHFileStatusCoordinator.updateFailure(hhFilePath, e.toString());
      }
      try {
        dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
        dataOutputStream.flush();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      e.printStackTrace();
    } finally {
      DataDistributorStarter.noOfAvailableDataDistributors.incrementAndGet();
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

