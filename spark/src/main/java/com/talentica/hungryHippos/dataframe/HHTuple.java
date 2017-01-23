/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

/**
 * This class stores the values for particular row with defined data type for each column. In other
 * words, it is simply a bean class to store the tuple information from the file system for each
 * row.
 * 
 * @author pooshans
 *
 */
public class HHTuple {

  private String key1;
  private String key2;
  private String key3;
  private String key4;
  private String key5;
  private String key6;
  private double key7;
  private double key8;
  private String key9;

  public String getKey1() {
    return key1;
  }

  public void setKey1(String key1) {
    this.key1 = key1;
  }

  public String getKey2() {
    return key2;
  }

  public void setKey2(String key2) {
    this.key2 = key2;
  }

  public String getKey3() {
    return key3;
  }

  public void setKey3(String key3) {
    this.key3 = key3;
  }

  public String getKey4() {
    return key4;
  }

  public void setKey4(String key4) {
    this.key4 = key4;
  }

  public String getKey5() {
    return key5;
  }

  public void setKey5(String key5) {
    this.key5 = key5;
  }

  public String getKey6() {
    return key6;
  }

  public void setKey6(String key6) {
    this.key6 = key6;
  }

  public Double getKey7() {
    return key7;
  }

  public void setKey7(Double key7) {
    this.key7 = key7;
  }

  public Double getKey8() {
    return key8;
  }

  public void setKey8(Double key8) {
    this.key8 = key8;
  }

  public String getKey9() {
    return key9;
  }

  public void setKey9(String key9) {
    this.key9 = key9;
  }

}
