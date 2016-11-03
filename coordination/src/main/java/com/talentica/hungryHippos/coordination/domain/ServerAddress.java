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
