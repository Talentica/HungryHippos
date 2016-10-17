package com.talentica.hungryHippos.test.knn;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class IndexCalculator implements Serializable {

  /**
  * 
  */
  private static final long serialVersionUID = 6864220909237020006L;
  int rowNum;
  int colNum;

  public IndexCalculator(int rowNum, int colNum) {
    this.rowNum = rowNum;
    this.colNum = colNum;
  }

  public Set<Point> getIndexes(Point topLeft, Point bottomRight) {
    Set<Point> result = new HashSet<>();
    int leftI = topLeft.getI() - 1;
    int leftJ = topLeft.getJ() - 1;
    int rightI = bottomRight.getI() + 1;
    int rightJ = bottomRight.getJ() + 1;
    int i = leftI;
    int j = rightI;
    while (i <= j) {
      if (leftJ < 0) {
        break;
      }
      if (i < 0) {
        i = topLeft.getI();
      }
      if (j >= rowNum) {
        j = bottomRight.getI();
      }
      result.add(new Point(i, leftJ));
      i++;
    }
    i = leftJ;
    j = rightJ;
    while (i <= j) {
      if (rightI >= rowNum) {
        break;
      }
      if (i < 0) {
        i = topLeft.getJ();
      }
      if (j >= colNum) {
        j = bottomRight.getJ();
      }
      result.add(new Point(rightI, i));
      i++;
    }
    i = rightI;
    j = leftI;
    while (i >= j) {
      if (rightJ >= colNum) {
        break;
      }
      if (i >= rowNum) {
        i = bottomRight.getI();
      }
      if (j < 0) {
        j = topLeft.getI();
      }
      result.add(new Point(i, rightJ));
      i--;
    }
    i = rightJ;
    j = leftJ;
    while (i >= j) {
      if (leftI < 0) {
        break;
      }
      if (i >= colNum) {
        i = bottomRight.getJ();
      }
      if (j < 0) {
        j = topLeft.getJ();
      }
      result.add(new Point(leftI, i));
      i--;
    }
    return result;
  }
}
