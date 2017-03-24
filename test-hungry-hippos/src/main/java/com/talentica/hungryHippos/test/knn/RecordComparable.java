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

public class RecordComparable implements Comparable<RecordComparable>, Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 7084528036494317432L;
  private Record record;
  private double distance;

  public RecordComparable(Record record, double distance) {
    this.record = record;
    this.distance = distance;
  }

  @Override
  public int compareTo(RecordComparable that) {
    return Double.valueOf(that.distance).compareTo(distance);
  }

  public Record getRecord() {
    return record;
  }

  public void setRecord(Record record) {
    this.record = record;
  }

  public double getDistance() {
    return distance;
  }

  public void setDistance(double distance) {
    this.distance = distance;
  }
}
