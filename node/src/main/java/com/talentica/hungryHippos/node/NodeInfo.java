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
package com.talentica.hungryHippos.node;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;

/**
 * {@code NodeInfo}
 * 
 * @author rajkishoreh
 * @since 22/7/16.
 */
public enum NodeInfo {
  INSTANCE;

  private String ip;
  private String id;
  private int identifier;
  private int port;

  NodeInfo() {
    ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
    List<Node> nodeList = config.getNode();
    Enumeration<NetworkInterface> e;
    boolean configPresent = false;
    try {
      e = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e1) {
      throw new AssertionError(e1);
    }
    searchIp: while (e.hasMoreElements()) {
      NetworkInterface n = e.nextElement();
      Enumeration<InetAddress>  ee = n.getInetAddresses();
      while (ee.hasMoreElements()) {
        InetAddress inetAddress = (InetAddress) ee.nextElement();
        for (Node node : nodeList) {
          if (inetAddress.getHostAddress().equals(node.getIp())
              || inetAddress.getHostName().equals(node.getIp())) {
            this.ip = node.getIp();
            this.identifier = node.getIdentifier();
            this.id = node.getIdentifier() + "";
            this.port = Integer.parseInt(node.getPort());
            configPresent = true;
            break searchIp;
          }
        }
      }
    }
    if (!configPresent) {
      throw new RuntimeException("Unable to find Corresponding Node in Cluster Configuration");
    }
  }

  public String getIp() {
    return ip;
  }

  public String getId() {
    return id;
  }

  public int getIdentifier() {
    return identifier;
  }

  public int getPort() {
    return port;
  }
}
