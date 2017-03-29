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

// TODO: Auto-generated Javadoc
/**
 * The Class HHRDDPartition.
 *
 * @author pooshans
 */
public class HHRDDPartition implements Partition {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = -8600257810541979113L;
  
  /** The nod id to ip. */
  private final Map<Integer, SerializedNode> nodIdToIp;
  
  /** The index. */
  private int index;
  
  /** The file path. */
  private String filePath;
  
  /** The data description. */
  private FieldTypeArrayDataDescription dataDescription;
  
  /** The rdd id. */
  private int rddId;
  
  /** The files. */
  private List<Tuple2<String,int[]>> files;
  
  /** The preferred hosts. */
  private List<String> preferredHosts;

  /**
   * Instantiates a new HHRDD partition.
   *
   * @param rddId the rdd id
   * @param index the index
   * @param filePath the file path
   * @param dataDescription the data description
   * @param preferredHosts the preferred hosts
   * @param files the files
   * @param nodIdToIp the nod id to ip
   */
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

  /* (non-Javadoc)
   * @see org.apache.spark.Partition#index()
   */
  @Override
  public int index() {
    return this.index;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return 31 * (31 + rddId) + index;
  }


  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HHRDDPartition)) {
      return false;
    }
    return ((HHRDDPartition) obj).index == index;
  }

  /**
   * Gets the file path.
   *
   * @return the file path
   */
  public String getFilePath() {
    return filePath;
  }

  /**
   * Gets the row size.
   *
   * @return the row size
   */
  public int getRowSize() {
    return dataDescription.getSize();
  }

  /**
   * Gets the field type array data description.
   *
   * @return the field type array data description
   */
  public FieldTypeArrayDataDescription getFieldTypeArrayDataDescription() {
    return this.dataDescription;
  }

  /**
   * Gets the preferred hosts.
   *
   * @return the preferred hosts
   */
  public List<String> getPreferredHosts() {
    return preferredHosts;
  }

  /**
   * Gets the files.
   *
   * @return the files
   */
  public List<Tuple2<String,int[]>> getFiles() {
    return files;
  }

  /**
   * Gets the nod id to ip.
   *
   * @return the nod id to ip
   */
  public Map<Integer, SerializedNode> getNodIdToIp() {
    return nodIdToIp;
  }
}
