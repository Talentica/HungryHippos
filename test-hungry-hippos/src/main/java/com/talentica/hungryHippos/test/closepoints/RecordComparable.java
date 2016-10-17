package com.talentica.hungryHippos.test.closepoints;

public class RecordComparable implements Comparable<RecordComparable>{

  Record record;
  double distance;
  
  public RecordComparable(Record record,double distance){
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
  
  

}
