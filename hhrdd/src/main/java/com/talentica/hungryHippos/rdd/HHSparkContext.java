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
package com.talentica.hungryHippos.rdd;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.DataDescription;

/**
 * {@code HHSparkContext} extends {@link JavaSparkContext} and provides additional support to
 * interact with HungryHippo file system.
 */
public class HHSparkContext extends JavaSparkContext {

  /** The hhrdd info cache. */
  private Map<String, HHRDDInfo> hhrddInfoCache = new HashMap<>();

  /**
   * Creates and returns an instance of {@link HHSparkContext} and connects to the zookeeper.
   *
   * @param config        a org.apache.spark.SparkConf object specifying Spark parameters
   * @param clientConfigurationFilePath        a path to the client-config.xml for connecting to the zookeeper.
   * @throws FileNotFoundException when client-config configuration file is not found
   * @throws JAXBException when the client-config configuration is not properly set
   * for the HungryHippo file
   */
  public HHSparkContext(SparkConf config, String clientConfigurationFilePath)
      throws FileNotFoundException, JAXBException {
    super(config);
    HHRDDHelper.initialize(clientConfigurationFilePath);
  }

  public HHSparkContext(SparkContext sc, String clientConfigurationFilePath)
          throws FileNotFoundException, JAXBException {
    super(sc);
    HHRDDHelper.initialize(clientConfigurationFilePath);
  }

  /**
   * Returns a {@link JavaRDD} of byte array from HungryHippo file system.
   *
   * @param jobDimensions        - An Integer array of indexes of the columns on which group by will be performed.
   *        Default dimension will be the first column.
   * @param hhFilePath        - Path to the HungryHippo file
   * @param requiresShuffle        - A value true will create optimal partitions of nearly 128MB each if possible.
   *        A value false will create optimal partitions according to the job dimensions.
   * @return JavaRDD  byte[] instance 
   * @throws JAXBException when the sharding-table configuration is not properly set
   * for the HungryHippo file
   * @throws IOException Any of the usual Input/Output related exceptions.
   */
  public JavaRDD<byte[]> binaryRecords(Integer[] jobDimensions, String hhFilePath, boolean requiresShuffle)
          throws JAXBException, IOException {
    HHRDDInfo hhrddInfo = getHHRDDInfo(hhFilePath);
    return new HHRDD(this.sc(), hhrddInfo, jobDimensions, requiresShuffle).toJavaRDD();
  }

  /**
   * Returns a {@link JavaRDD} of byte array from HungryHippo file system.
   *
   * @param jobDimensions        - An Integer array of indexes of the columns on which group by will be performed.
   *        Default dimension will be the first column.
   * @param hhFilePath        - Path to the HungryHippo file
   * @param requiresShuffle        - A value true will create optimal partitions of nearly 128MB each if possible.
   *        A value false will create optimal partitions according to the job dimensions.
   * @return JavaRDD  byte[] instance
   * @throws JAXBException when the sharding-table configuration is not properly set
   * for the HungryHippo file
   * @throws IOException Any of the usual Input/Output related exceptions.
   */
 /* public JavaRDD<HHRDDRowReader> rowReaderRecords(Integer[] jobDimensions, String hhFilePath, boolean requiresShuffle)
          throws JAXBException, IOException {
    HHRDDInfo hhrddInfo = getHHRDDInfo(hhFilePath);
    return new HHRDDOpt(this.sc(), hhrddInfo, jobDimensions, requiresShuffle).toJavaRDD();
  }*/

  /**
   * Returns an instance of {@link HHRDDInfo} type.
   *
   * @param hhFilePath        Path to the HungryHippo file
   * @return HHRDDInfo instance
   * @throws JAXBException when the sharding-table configuration is not properly set
   * for the HungryHippo file
   * @throws IOException Any of the usual Input/Output related exceptions.
   */
  private HHRDDInfo getHHRDDInfo(String hhFilePath)
          throws JAXBException, IOException {
    HHRDDInfo hhrddInfo = hhrddInfoCache.get(hhFilePath);
    if (hhrddInfo == null) {
      hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);
      hhrddInfoCache.put(hhFilePath, hhrddInfo);
    }
    return hhrddInfo;
  }

  /**
   * Returns an instance of broadcast {@link DataDescription}.
   *
   * @param hhFilePath        Path to the HungryHippo file
   * @return Broadcast DataDescription instance
   * @throws JAXBException when the sharding-table configuration is not properly set
   * for the HungryHippo file
   * @throws IOException Any of the usual Input/Output related exceptions.
   */
  public Broadcast<DataDescription> broadcastFieldDataDescription(String hhFilePath)
          throws JAXBException, IOException {
    return broadcast(getHHRDDInfo(hhFilePath).getFieldDataDesc());
  }

  /**
   * Returns an instance of broadcast {@link DataDescription}.
   *
   * @param hhFilePath        Path to the HungryHippo file
   * @return Broadcast DataDescription instance
   * @throws JAXBException when the sharding-table configuration is not properly set
   * for the HungryHippo file
   * @throws IOException Any of the usual Input/Output related exceptions.
   */
  public DataDescription getFieldDataDescription(String hhFilePath)
          throws JAXBException, IOException {
    return getHHRDDInfo(hhFilePath).getFieldDataDesc();
  }

  /**
   * Returns array of column indexes on which sharding has been performed.
   *
   * @param hhFilePath      Path to the HungryHippo file
   * @return the sharding indexes
   * @throws JAXBException when the sharding-table configuration is not properly set
   * for the HungryHippo file
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public int[] getShardingIndexes(String hhFilePath)
          throws JAXBException, IOException {
    return getHHRDDInfo(hhFilePath).getShardingIndexes();
  }

  /**
   * Returns an absolute path inside HungryHippo file system for the path.
   *
   * @param path      Path to the HungryHippo file
   * @return absolute path
   */
  public String getActualPath(String path) {
    return HHRDDHelper.getActualPath(path);
  }


}
