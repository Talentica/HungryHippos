/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.Partition;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class HHRDDPartition implements Partition {

  private static final long serialVersionUID = -8600257810541979113L;
  private final Map<Integer, String> nodIdToIp;
  private int index;
  private String filePath;
  private FieldTypeArrayDataDescription dataDescription;
  private int rddId;
  private List<Tuple2<String,int[]>> files;
  private String preferredHost;

  public HHRDDPartition(int rddId , int index, String filePath,
                        FieldTypeArrayDataDescription dataDescription, String preferredHost , List<Tuple2<String,int[]>> files, Map<Integer,String> nodIdToIp) {
    this.index = index;
    this.filePath = filePath;
    this.dataDescription = dataDescription;
    this.rddId = rddId;
    this.preferredHost = preferredHost;
    this.files = files;
    this.nodIdToIp = nodIdToIp;
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

  public int getRowSize() {
    return dataDescription.getSize();
  }

  public FieldTypeArrayDataDescription getFieldTypeArrayDataDescription() {
    return this.dataDescription;
  }

  public String getPreferredHost() {
    return preferredHost;
  }

  public List<Tuple2<String,int[]>> getFiles() {
    return files;
  }

  public Map<Integer, String> getNodIdToIp() {
    return nodIdToIp;
  }
}
