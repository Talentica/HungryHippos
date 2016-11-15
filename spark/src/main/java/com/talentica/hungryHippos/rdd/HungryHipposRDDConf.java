/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.rdd.utility.ShardingApplicationContext;


/**
 * @author pooshans
 *
 */
public class HungryHipposRDDConf implements Serializable {

  private static final long serialVersionUID = -9079703351777187673L;
  private int buckets;
  private int rowSize;
  private int[] shardingIndexes;
  private String directoryLocation;
  private DataDescription dataDescription;

  public HungryHipposRDDConf(int buckets, String shardingFolderPath, String directoryLocation) {
    this.buckets = buckets;
    ShardingApplicationContext context = new ShardingApplicationContext(shardingFolderPath);
    this.dataDescription = context.getConfiguredDataDescription();
    this.rowSize = dataDescription.getSize();
    this.shardingIndexes = context.getShardingIndexes();
    this.directoryLocation = directoryLocation;
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

  public String getDirectoryLocation() {
    return directoryLocation;
  }

  public void setDirectoryLocation(String directoryLocation) {
    this.directoryLocation = directoryLocation;
  }

  public DataDescription getDataDescription() {
    return this.dataDescription;
  }

}
