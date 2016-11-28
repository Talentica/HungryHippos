/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.File;

import org.apache.spark.Partition;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;

/**
 * @author pooshans
 *
 */
public class HHRDDPartition implements Partition {

  private static final long serialVersionUID = -8600257810541979113L;
  private int partitionId;
  private String filePath;
  private FieldTypeArrayDataDescription dataDescription;
  private String fileName;

  public HHRDDPartition(int partitionId, String filePath,
      FieldTypeArrayDataDescription dataDescription) {
    this.partitionId = partitionId;
    this.filePath = filePath;
    this.dataDescription = dataDescription;
    this.fileName = new File(filePath).getName();
  }

  @Override
  public int index() {
    return this.partitionId;
  }

  @Override
  public int hashCode() {
    return fileName.toString().hashCode();
  }


  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HHRDDPartition)) {
      return false;
    }
    return ((HHRDDPartition) obj).fileName.equals(fileName);
  }

  public String getFilePath() {
    return filePath;
  }

  public String getFileName() {
    return fileName;
  }

  public int getRowSize() {
    return dataDescription.getSize();
  }

  public FieldTypeArrayDataDescription getFieldTypeArrayDataDescription() {
    return this.dataDescription;
  }

  public int getPartitionId() {
    return partitionId;
  }


}
