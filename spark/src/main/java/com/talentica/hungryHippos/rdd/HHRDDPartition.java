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
  private int index;
  private String filePath;
  private FieldTypeArrayDataDescription dataDescription;
  private String fileName;
  private int rddId;
  
  public HHRDDPartition(int rddId , int index, String filePath,
      FieldTypeArrayDataDescription dataDescription) {
    this.index = index;
    this.filePath = filePath;
    this.dataDescription = dataDescription;
    this.fileName = new File(filePath).getName();
    this.rddId = rddId;
  }

  @Override
  public int index() {
    return this.index;
  }

  @Override
  public int hashCode() {
    return 31 * (31 + rddId) + index;
  }


  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HHRDDPartition)) {
      return false;
    }
    return ((HHRDDPartition) obj).index == index;
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


}
