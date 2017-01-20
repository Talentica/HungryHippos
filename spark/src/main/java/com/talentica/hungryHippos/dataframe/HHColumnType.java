package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;

public class HHColumnType implements Serializable {
  private static final long serialVersionUID = 6864080436196699506L;
  public String name;
  public String dataType;
  public int length;

  public HHColumnType(String name, String dataType, int length) {
    this.name = name;
    this.dataType = dataType;
    this.length = length;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

}
