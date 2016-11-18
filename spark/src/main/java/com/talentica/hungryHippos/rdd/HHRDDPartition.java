/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.File;

import org.apache.spark.Partition;

import com.talentica.hungryHippos.client.domain.DataDescription;

/**
 * @author pooshans
 *
 */
public class HHRDDPartition implements Partition {

  private static final long serialVersionUID = -8600257810541979113L;
  private int partitionId;
  private String filePath;
  private DataDescription dataDescription;
  private String fileName;

  public HHRDDPartition(int partitionId, String filePath, DataDescription dataDescription) {
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
  public boolean org$apache$spark$Partition$$super$equals(Object obj) {
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

  public DataDescription getDataDescription() {
    return this.dataDescription;
  }

}
