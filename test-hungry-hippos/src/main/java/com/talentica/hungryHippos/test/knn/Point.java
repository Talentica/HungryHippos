package com.talentica.hungryHippos.test.knn;

import java.io.Serializable;

public class Point implements Serializable {

  /**
  * 
  */
  private static final long serialVersionUID = -570740979840515824L;
  private int i;
  private int j;

  public Point() {}

  public Point(int i, int j) {
    this.i = i;
    this.j = j;
  }

  public int getI() {
    return i;
  }

  public void setI(int i) {
    this.i = i;
  }

  public int getJ() {
    return j;
  }

  public void setJ(int j) {
    this.j = j;
  }

  @Override
  public String toString() {
    return "(" + i + "," + j + ")";
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass())
      return false;
    Point point = (Point) that;
    if (this.i == point.i && this.j == point.j) {
      return true;
    }
    return false;
  }

  public int hashCode() {
    int hash = i + j;
    return hash;
  }

}
