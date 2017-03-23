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
 * {@code ServerAddress} used keeping the details about the server/machine.
 * 
 * @author PooshanS
 *
 */
public class ServerAddress {

  private String ip;
  private String hostname;

  /**
   * creates an instance of ServerAddress with empty values.
   */
  ServerAddress() {}

  /**
   * creates an instance of ServerAddress with {@value hostname} and {@value ip}.
   * 
   * @param hostname
   * @param ip
   */
  public ServerAddress(String hostname, String ip) {
    this.hostname = hostname;
    this.ip = ip;
  }

  /**
   * retrieves the host name associated with this instance.
   * 
   * @return a String.
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * sets hostname to this instance.
   * 
   * @param hostname
   */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * retrieves the ip associated with this instance.
   * 
   * @return
   */
  public String getIp() {
    return ip;
  }

  /**
   * sets a new ip to this instance.
   * 
   * @param ip
   */
  public void setIp(String ip) {
    this.ip = ip;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ServerAddress that = (ServerAddress) o;

    if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null)
      return false;
    if (!ip.equals(that.ip))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = ip.hashCode();
    result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
    return result;
  }

}
