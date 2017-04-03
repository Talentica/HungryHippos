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
package com.talentica.hungryHippos.rdd;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class SerializedNode.
 */
public class SerializedNode implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 7595006167742376795L;
  
  /** The id. */
  private int id;
  
  /** The ip. */
  private String ip;
  
  /** The port. */
  private int port;

  /**
   * Instantiates a new serialized node.
   *
   * @param id the id
   * @param ip the ip
   * @param port the port
   */
  public SerializedNode(int id, String ip,int port) {
    super();
    this.id = id;
    this.ip = ip;
    this.port = port;
  }

  /**
   * Gets the id.
   *
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * Gets the ip.
   *
   * @return the ip
   */
  public String getIp() {
    return ip;
  }

  /**
   * Gets the port.
   *
   * @return the port
   */
  public int getPort() {
    return port;
  }

}
