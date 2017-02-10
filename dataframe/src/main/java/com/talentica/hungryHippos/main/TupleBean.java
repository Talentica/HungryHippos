/**
 * 
 */
package com.talentica.hungryHippos.main;

import com.talentica.hungryHippos.dataframe.HHField;

/**
 * It is and example and supposed to be written by client. This class stores the values for
 * particular row with defined data type for each column. In other words, it is simply a bean class
 * to store the tuple information from the file system for each row. Property name could be as per
 * user convenient. However, user is supposed to provide proper index by annotating
 * {@code HHFieldOrder} over field of the as per data stored in file system.
 * 
 * @author pooshans
 * @since 25/01/2017
 */
public class TupleBean {

  @HHField(index = 0)
  private String col1;
  @HHField(index = 1)
  private String col2;
  @HHField(index = 2)
  private String col3;
  @HHField(index = 3)
  private String col4;
  @HHField(index = 4)
  private String col5;
  @HHField(index = 5)
  private String col6;
  @HHField(index = 6)
  private double col7;
  @HHField(index = 7)
  private double col8;
  @HHField(index = 8)
  private String col9;

  public String getCol1() {
    return col1;
  }

  public void setCol1(String col1) {
    this.col1 = col1;
  }

  public String getCol2() {
    return col2;
  }

  public void setCol2(String col2) {
    this.col2 = col2;
  }

  public String getCol3() {
    return col3;
  }

  public void setCol3(String col3) {
    this.col3 = col3;
  }

  public String getCol4() {
    return col4;
  }

  public void setCol4(String col4) {
    this.col4 = col4;
  }

  public String getCol5() {
    return col5;
  }

  public void setCol5(String col5) {
    this.col5 = col5;
  }

  public String getCol6() {
    return col6;
  }

  public void setCol6(String col6) {
    this.col6 = col6;
  }

  public double getCol7() {
    return col7;
  }

  public void setCol7(double col7) {
    this.col7 = col7;
  }

  public double getCol8() {
    return col8;
  }

  public void setCol8(double col8) {
    this.col8 = col8;
  }

  public String getCol9() {
    return col9;
  }

  public void setCol9(String col9) {
    this.col9 = col9;
  }

}
