/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.DataInputStream;

import org.apache.spark.Partition;

/**
 * @author pooshans
 *
 */
public class HungryHipposRDDPartition implements Partition {

  private int partitionId;
  private DataInputStream dataInputStream;
  private int rowSize;

  public HungryHipposRDDPartition(int partitionId, DataInputStream dataInputStream, int rowSize) {
    this.partitionId = partitionId;
    this.dataInputStream = dataInputStream;
    this.rowSize = rowSize;
  }

  @Override
  public int index() {
    return this.partitionId;
  }

  @Override
  public boolean org$apache$spark$Partition$$super$equals(Object obj) {
    if (!(obj instanceof HungryHipposRDDPartition)) {
      return false;
    }
    return ((HungryHipposRDDPartition) obj).partitionId != partitionId;
  }

  public DataInputStream getDataInputStream() {
    return dataInputStream;
  }

  public int getRowSize() {
    return rowSize;
  }

}
