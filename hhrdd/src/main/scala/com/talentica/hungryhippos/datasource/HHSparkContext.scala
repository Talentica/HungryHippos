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

package com.talentica.hungryhippos.datasource

import java.io.IOException
import java.util
import javax.xml.bind.JAXBException

import com.talentica.hungryHippos.client.domain.DataDescription
import com.talentica.hungryhippos.datasource.rdd.{HHRDDHelper, HHRDDInfo}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast


/**
  * Created by rajkishoreh.
  * {@code HHSparkContext} extends {@link JavaSparkContext} and provides additional support to
  * interact with HungryHippo file system.
  */
class HHSparkContext(context: SparkContext, clientConfigurationFilePath: String)
  extends JavaSparkContext(context) {
  HHRDDHelper.initialize(clientConfigurationFilePath)
  /** The hhrdd info cache. */
  private val hhrddInfoCache = new util.HashMap[String, HHRDDInfo]


  /**
    * Returns an instance of {@link HHRDDInfo} type.
    *
    * @param hhFilePath Path to the HungryHippo file
    * @return HHRDDInfo instance
    * @throws JAXBException when the sharding-table configuration is not properly set
    *                       for the HungryHippo file
    * @throws IOException   Any of the usual Input/Output related exceptions.
    */
  @throws[JAXBException]
  @throws[IOException]
  private def getHHRDDInfo(hhFilePath: String) = {
    var hhrddInfo = hhrddInfoCache.get(hhFilePath)
    if (hhrddInfo == null) {
      hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath)
      hhrddInfoCache.put(hhFilePath, hhrddInfo)
    }
    hhrddInfo
  }

  /**
    * Returns an instance of broadcast {@link DataDescription}.
    *
    * @param hhFilePath Path to the HungryHippo file
    * @return Broadcast DataDescription instance
    * @throws JAXBException when the sharding-table configuration is not properly set
    *                       for the HungryHippo file
    * @throws IOException   Any of the usual Input/Output related exceptions.
    */
  @throws[JAXBException]
  @throws[IOException]
  def broadcastFieldDataDescription(hhFilePath: String): Broadcast[DataDescription] = broadcast(getHHRDDInfo(hhFilePath).getFieldDataDesc)

  @throws[JAXBException]
  @throws[IOException]
  def getFieldDataDescription(hhFilePath: String): DataDescription = getHHRDDInfo(hhFilePath).getFieldDataDesc

  /**
    * Returns array of column indexes on which sharding has been performed.
    *
    * @param hhFilePath Path to the HungryHippo file
    * @return the sharding indexes
    * @throws JAXBException when the sharding-table configuration is not properly set
    *                       for the HungryHippo file
    * @throws IOException   Signals that an I/O exception has occurred.
    */
  @throws[JAXBException]
  @throws[IOException]
  def getShardingIndexes(hhFilePath: String): Array[Int] = getHHRDDInfo(hhFilePath).getShardingIndexes

  /**
    * Returns an absolute path inside HungryHippo file system for the path.
    *
    * @param path Path to the HungryHippo file
    * @return absolute path
    */
  def getActualPath(path: String): String = HHRDDHelper.getActualPath(path)
}

