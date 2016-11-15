/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.Serializable;

/**
 * @author pooshans
 *
 */
public class HungryHipposRDDConf implements Serializable{
  
  private static final long serialVersionUID = -9079703351777187673L;
  private int buckets;
  private int rowSize;
  private int[] shardingIndexes;
  
  
  public HungryHipposRDDConf(int buckets, int rowSize, int[] shardingIndexes) {
    this.buckets = buckets;
    this.rowSize = rowSize;
    this.shardingIndexes = shardingIndexes;
  }
  
  public int getBuckets() {
    return buckets;
  }
  public void setBuckets(int buckets) {
    this.buckets = buckets;
  }
  public int getRowSize() {
    return rowSize;
  }
  public void setRowSize(int rowSize) {
    this.rowSize = rowSize;
  }
  public int[] getShardingIndexes() {
    return shardingIndexes;
  }
  public void setShardingIndexes(int[] shardingIndexes) {
    this.shardingIndexes = shardingIndexes;
  }
  
}
