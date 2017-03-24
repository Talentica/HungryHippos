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
package com.talentica.hungryHippos.sharding.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;

public class ShardingFileUploader implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ShardingFileUploader.class);
  private Node node;
  private boolean success;
  private String destinationPath;
  private String sourceFile;
  private int idealSizeOfBuffer = 8192;
  private long end;
  private long start;

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

    transferData();

    if (!this.success) {
      int retry = 0;
      while (retry < 5) {
        retry++;
        logger.warn("transfer of sharding table failed on  {} ", node.getIp());
        logger.info("retrying transfer  on {} ", node.getIp());
        transferData();
      }
    } else {
      logger.info("transfer of sharding table completed on  {} in {} ms", node.getIp(),
          ((end - start)));
    }
  }


  private void transferData() {
    DataInputStream dis = null;
    DataOutputStream dos = null;
    FileInputStream fis = null;
    Socket socket = null;
    try {
      socket = ServerUtils.connectToServer(node.getIp() + ":" + node.getPort(), 50);
      dis = new DataInputStream(socket.getInputStream());
      dos = new DataOutputStream(socket.getOutputStream());
      long size = Files.size(Paths.get(sourceFile));
      dos.writeInt(HungryHippoServicesConstants.ACCEPT_FILE);
      dos.writeUTF(destinationPath);
      dos.writeLong(size);
      dos.writeInt(HungryHippoServicesConstants.SHARDING_TABLE);

      byte[] buffer = new byte[idealSizeOfBuffer];
      int read = 0;
      fis = new FileInputStream(new File(sourceFile));
      start = System.currentTimeMillis();
      while ((read = fis.read(buffer)) != -1) {
        dos.write(buffer, 0, read);
      }
      dos.flush();

      this.success = dis.readBoolean();
      end = System.currentTimeMillis();
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


