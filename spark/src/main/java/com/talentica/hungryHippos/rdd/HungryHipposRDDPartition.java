/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.DataInputStream;

import org.apache.spark.Partition;

import com.talentica.hungryHippos.client.domain.DataDescription;

/**
 * @author pooshans
 *
 */
public class HungryHipposRDDPartition implements Partition {

  private static final long serialVersionUID = -8600257810541979113L;
  private int partitionId;
  private String filePath;
  private DataDescription dataDescription;

  public HungryHipposRDDPartition(int partitionId, String filePath,
      DataDescription dataDescription) {
    this.partitionId = partitionId;
    this.filePath = filePath;
    this.dataDescription = dataDescription;
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

  public String getFilePath() {
    return filePath;
  }

  public int getRowSize() {
    return dataDescription.getSize();
  }

  public DataDescription getDataDescription() {
    return this.dataDescription;
  }

}
