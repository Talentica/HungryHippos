package com.talentica.hungryHippos.rdd;

import java.io.Serializable;

public class SerializedNode implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 7595006167742376795L;
  private int id;
  private String ip;
  private int port;

  public SerializedNode(int id, String ip,int port) {
    super();
    this.id = id;
    this.ip = ip;
    this.port = port;
  }

  public int getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

}
