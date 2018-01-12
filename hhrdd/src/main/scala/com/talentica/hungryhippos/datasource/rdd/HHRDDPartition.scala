/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryhippos.datasource.rdd

import java.util

import org.apache.spark.Partition

/**
  * Created by rajkishoreh.
  */
class HHRDDPartition(rddId: Int, indexId: Int, preferredHosts: util.List[String],
                     files: util.List[FileDetail]) extends Partition {

  override def index = indexId;

  override def hashCode: Int = 31 * (31 + rddId) + index


  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[HHRDDPartition]) return false
    obj.asInstanceOf[HHRDDPartition].index == index
  }


  /**
    * Gets the preferred hosts.
    *
    * @return the preferred hosts
    */
  def getPreferredHosts: util.List[String] = preferredHosts

  /**
    * Gets the files.
    *
    * @return the files
    */
  def getFiles: util.List[FileDetail] = files

}
