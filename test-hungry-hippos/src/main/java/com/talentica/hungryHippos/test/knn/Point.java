/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
