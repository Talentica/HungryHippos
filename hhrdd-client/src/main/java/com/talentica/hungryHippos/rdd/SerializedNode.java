package com.talentica.hungryHippos.rdd;

import java.io.Serializable;

public class SerializedNode implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 7595006167742376795L;
  private int id;
  private String ip;

  public SerializedNode(int id, String ip) {
    super();
    this.id = id;
    this.ip = ip;
  }

  public int getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }


}
