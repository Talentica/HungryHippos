/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
  private final Map<Integer, SerializedNode> nodIdToIp;
  private int index;
  private String filePath;
  private FieldTypeArrayDataDescription dataDescription;
  private int rddId;
  private List<Tuple2<String,int[]>> files;
  private List<String> preferredHosts;

  public HHRDDPartition(int rddId , int index, String filePath,
                        FieldTypeArrayDataDescription dataDescription, List<String> preferredHosts, List<Tuple2<String,int[]>> files, Map<Integer,SerializedNode> nodIdToIp) {
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

  public Map<Integer, SerializedNode> getNodIdToIp() {
    return nodIdToIp;
  }
}
