/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.util.List;
import java.util.Map;

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
  private List<String> preferredHosts;

  public HHRDDPartition(int rddId , int index, String filePath,
                        FieldTypeArrayDataDescription dataDescription, List<String> preferredHosts, List<Tuple2<String,int[]>> files, Map<Integer,String> nodIdToIp) {
    this.index = index;
    this.filePath = filePath;
    this.dataDescription = dataDescription;
    this.rddId = rddId;
    this.preferredHosts = preferredHosts;
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

  public List<String> getPreferredHosts() {
    return preferredHosts;
  }

  public List<Tuple2<String,int[]>> getFiles() {
    return files;
  }

  public Map<Integer, String> getNodIdToIp() {
    return nodIdToIp;
  }
}
