/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.coordination.server;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code ServerUtils} is used for connecting to a server.
 * 
 * @author PooshanS
 *
 */
public class ServerUtils {

  public static final String COLON = ":";
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerUtils.class.getName());

  /**
   * used for connecting to a {@value server}. If it fails because of some reason System will try
   * reconnect itself until it reaches {@value numberOfAttempts}.
   * 
   * @param server
   * @param numberOfAttempts
   * @return {@link Socket}
   * @throws IOException
   * @throws InterruptedException
   */
  public static Socket connectToServer(String server, int numberOfAttempts)
      throws IOException, InterruptedException {
    int tryCount = 0;
    while (true) {
      try {
        tryCount++;
        Socket socket =
            new Socket(server.split(":")[0].trim(), Integer.valueOf(server.split(":")[1].trim()));
        return socket;
      } catch (ConnectException cex) {
        if (tryCount >= numberOfAttempts) {
          throw cex;
        }
        LOGGER.warn("Connection could not get established. Please start the node {}",
            server.split(":")[0].trim());
        Thread.sleep(5000);
      }
    }
  }
}
