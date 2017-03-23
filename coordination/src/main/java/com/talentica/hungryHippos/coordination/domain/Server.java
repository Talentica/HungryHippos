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
/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

/**
 * {@code Server} used for tracking Server associated with system.
 * 
 * @author PooshanS
 *
 */
public class Server {

  public enum ServerStatus {
    ACTIVE, INACTIVE;
  };

  ServerAddress serverAddress;

  int port;

  int ttlSeconds;

  int maxMissed;

  String serverType;

  String description;

  Object data;

  ServerStatus serverStatus;

  String currentDateTime;

  int id;

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Server server = (Server) o;

    if (port != server.port)
      return false;
    if (!serverAddress.equals(server.serverAddress))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = serverAddress.hashCode();
    result = 31 * result + port;
    return result;
  }

  @Override
  public String toString() {
    return getName() + " :: " + getDescription();
  }

  /**
   * retrieves the host name of the node/machine associated.
   * 
   * @return a String.
   */
  public String getName() {
    return serverAddress.getHostname();
  }

  /**
   * retrieves the description of the server if any. else null.
   * 
   * @return a String.
   */
  public String getDescription() {
    return description;
  }

  /**
   * sets the description.
   * 
   * @param description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * sets the port.
   * 
   * @param port
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * retrieves a {@link ServerAddress}.
   * 
   * @return a instance of ServerAddress.
   */
  public ServerAddress getServerAddress() {
    return serverAddress;
  }

  /**
   * sets a serverAddress.
   * 
   * @param serverAddress
   */
  public void setServerAddress(ServerAddress serverAddress) {
    this.serverAddress = serverAddress;
  }

  /**
   * retrieves the server type.
   * 
   * @return
   */
  public String getServerType() {
    return serverType;
  }

  /**
   * sets the server type.
   * 
   * @param serverType
   */
  public void setServerType(String serverType) {
    this.serverType = serverType;
  }

  /**
   * retrieves the port.
   * 
   * @return an int.
   */
  public int getPort() {
    return port;
  }

  /**
   * retrieves the maximum missed value.
   * 
   * @return an int.
   */
  public int getMaxMissed() {

    return maxMissed;
  }

  /**
   * sets the maximum missed value.
   * 
   * @param maxMissed
   */
  public void setMaxMissed(int maxMissed) {
    this.maxMissed = maxMissed;
  }

  /**
   * retrieves the time to live in seconds.
   * 
   * @return int.
   */
  public int getTtlSeconds() {
    return ttlSeconds;
  }

  /**
   * sets time to live in seconds.
   * 
   * @param ttlSeconds
   */
  public void setTtlSeconds(int ttlSeconds) {
    this.ttlSeconds = ttlSeconds;
  }

  /**
   * retrieves the object data.
   * 
   * @return Object.
   */
  public Object getData() {
    return data;
  }

  /**
   * sets the data.
   * 
   * @param data
   */
  public void setData(Object data) {
    this.data = data;
  }

  /**
   * retrieves the {@link ServerStatus}.
   * 
   * @return a ServerStatus.
   */
  public ServerStatus getServerStatus() {
    return serverStatus;
  }

  /**
   * sets the {@link ServerStatus}.
   * 
   * @param serverStatus
   */
  public void setServerStatus(ServerStatus serverStatus) {
    this.serverStatus = serverStatus;
  }

  /**
   * retrieves the current Date time.
   * 
   * @return a String.
   */
  public String getCurrentDateTime() {
    return currentDateTime;
  }

  /**
   * sets the Current Date time.
   * 
   * @param currentDateTime
   */
  public void setCurrentDateTime(String currentDateTime) {
    this.currentDateTime = currentDateTime;
  }

  /**
   * Default constructor, only useful for the JSON deserializer. Should not be used, but may be
   * useful for serializers, injections, etc.
   */
  public Server() {
    this(new ServerAddress(), 0, 0);
  }

  /**
   * Constructs a server, listening on a given port, requiring a ping every @code{ttlSeconds}
   *
   * @param serverAddress server's hostname and address
   * @param port where the server is operating
   * @param ttlSeconds interval between pings
   */
  public Server(ServerAddress serverAddress, int port, int ttlSeconds) {
    this.port = port;
    this.serverAddress = serverAddress;
    this.ttlSeconds = ttlSeconds;
  }

  /**
   * retrieves the id.
   * 
   * @return int.
   */
  public int getId() {
    return id;
  }

  /**
   * sets the Id.
   * 
   * @param id
   */
  public void setId(int id) {
    this.id = id;
  }


}
