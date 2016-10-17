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
