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
package com.talentica.hungryhippos.filesystem.server;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * {@code DataRequestHandlerServer} is for creating a Server which listens at a particular port and
 * creates Threads for handling client requests
 * <p>
 * 
 * @author rajkishoreh
 * @since 30/6/16.
 */
public class DataRequestHandlerServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataRequestHandlerServer.class);

  private int port;
  private int maximumClientRequests;
  private DataRetrievalThread[] requestHandlingThreads;

  public DataRequestHandlerServer(int port, int maximumClientRequests) {
    this.port = port;
    this.maximumClientRequests = maximumClientRequests;
    this.requestHandlingThreads = new DataRetrievalThread[maximumClientRequests];
  }

  /**
   * This method starts a server which creates Threads for clients requests coming for Data
   * Retrieval
   *
   * @throws IOException
   */
  public void start() throws IOException {

    ServerSocket serverSocket = new ServerSocket(port);
    while (true) {
      try {
        LOGGER.info("[{}] Waiting for client on port {} ...", Thread.currentThread().getName(),
            serverSocket.getLocalPort());
        Socket clientSocket = serverSocket.accept();
        int i = 0;
        for (i = 0; i < maximumClientRequests; i++) {
          if (requestHandlingThreads[i] == null || !requestHandlingThreads[i].isAlive()) {
            LOGGER.info("[{}] Assigning slot {}", Thread.currentThread().getName(), i);
            (requestHandlingThreads[i] = new DataRetrievalThread(clientSocket,
                FileSystemContext.getRootDirectory(), FileSystemContext.getFileStreamBufferSize()))
                    .start();
            break;
          }
        }
        if (i == maximumClientRequests) {
          LOGGER.info("[{}] Server too busy. Try Later", Thread.currentThread().getName());
          DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
          dos.writeUTF(FileSystemConstants.DATA_SERVER_BUSY);
          clientSocket.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * This is the main method to start the DataRequestHandlerServer
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException, InterruptedException,
      ClassNotFoundException, KeeperException, JAXBException {
    ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(args[0], ClientConfig.class);
    String connectString = clientConfig.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
    HungryHippoCurator curator = HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    int port = FileSystemContext.getServerPort();
    int maximumClientRequests = FileSystemContext.getMaxClientRequests();
    new DataRequestHandlerServer(port, maximumClientRequests).start();
  }

}
