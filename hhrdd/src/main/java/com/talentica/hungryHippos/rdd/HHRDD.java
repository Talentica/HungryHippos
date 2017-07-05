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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * The Class HHRDD.
 *
 * @author pooshans
 */
class HHRDD extends RDD<byte[]> implements Serializable {
  
  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 4074885953480955556L;
  
  /** The Constant HHRD_READER__TAG. */
  private static final ClassTag<byte[]> HHRD_READER__TAG =
      ClassManifestFactory$.MODULE$.fromClass(byte[].class);
  
  /** The id. */
  private int id;
  
  /** The partitions. */
  private Partition[] partitions;
  
  /** The hhrdd info. */
  private HHRDDInfo hhrddInfo;
  
  /** The temp dir attempts. */
  private static int TEMP_DIR_ATTEMPTS = 10000;
  
  /** The temp dir. */
  private static File tempDir = createTempDir();

  /**
   * Instantiates a new hhrdd.
   *
   * @param sc the sc
   * @param hhrddInfo the hhrdd info
   * @param jobDimensions the job dimensions
   * @param requiresShuffle the requires shuffle
   */
  public HHRDD(JavaSparkContext sc, HHRDDInfo hhrddInfo, Integer[] jobDimensions,
      boolean requiresShuffle) {
    super(sc.sc(), new ArrayBuffer<Dependency<?>>(), HHRD_READER__TAG);

    this.hhrddInfo = hhrddInfo;
    this.id = sc.sc().newRddId();

    String[] keyOrder = hhrddInfo.getKeyOrder();
    int[] shardingIndexes = hhrddInfo.getShardingIndexes();
    List<Integer> jobShardingDimensions = new ArrayList<>();
    List<String> jobShardingDimensionsKey = new ArrayList<>();


    String primaryDimensionKey = null;
    int jobPrimaryDimensionIdx = 0;
    int maxBucketSize = 0;
    int jobDimensionIdx = 0;
    int keyOrderIdx = 0;
    if(jobDimensions!=null && jobDimensions.length>0) {
      for (int i = 0; i < shardingIndexes.length; i++) {
        for (int j = 0; j < jobDimensions.length; j++) {
          if (shardingIndexes[i] == jobDimensions[j]) {
            int bucketSize = hhrddInfo.getBucketToNodeNumberMap().get(keyOrder[i]).size();
            String dimensionKey = keyOrder[i];
            jobShardingDimensions.add(shardingIndexes[i]);
            jobShardingDimensionsKey.add(keyOrder[i]);
            if (bucketSize > maxBucketSize) {
              keyOrderIdx=i;
              primaryDimensionKey = dimensionKey;
              jobPrimaryDimensionIdx = jobDimensionIdx;
            }
            jobDimensionIdx++;
          }
        }
      }
    }
    if (jobShardingDimensions.isEmpty()) {
      jobPrimaryDimensionIdx = shardingIndexes[0];
      primaryDimensionKey = keyOrder[0];
      jobShardingDimensions.add(jobPrimaryDimensionIdx);
      jobShardingDimensionsKey.add(primaryDimensionKey);
      jobDimensionIdx++;
    }

    int noOfExecutors = sc.defaultParallelism();
    if (requiresShuffle) {
      this.partitions = hhrddInfo.getOptimizedPartitions(id, noOfExecutors, jobShardingDimensions,
          jobPrimaryDimensionIdx, jobShardingDimensionsKey, primaryDimensionKey+keyOrderIdx);
    } else {
      this.partitions = hhrddInfo.getPartitions(id, noOfExecutors, jobShardingDimensions,
          jobPrimaryDimensionIdx, jobShardingDimensionsKey, primaryDimensionKey);

    }

  }

  /* (non-Javadoc)
   * @see org.apache.spark.rdd.RDD#compute(org.apache.spark.Partition, org.apache.spark.TaskContext)
   */
  @Override
  public Iterator<byte[]> compute(Partition partition, TaskContext taskContext) {
    HHRDDPartition hhRDDPartion = (HHRDDPartition) partition;
    HHRDDIterator iterator = null;
    try {
      iterator = new HHRDDIterator(hhRDDPartion.getFilePath(), hhRDDPartion.getRowSize(),
          hhRDDPartion.getFiles(), hhRDDPartion.getNodIdToIp(),tempDir);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return iterator;
  }

  /* (non-Javadoc)
   * @see org.apache.spark.rdd.RDD#getPartitions()
   */
  @Override
  public Partition[] getPartitions() {
    return this.partitions;
  }

  /* (non-Javadoc)
   * @see org.apache.spark.rdd.RDD#getPreferredLocations(org.apache.spark.Partition)
   */
  @Override
  public Seq<String> getPreferredLocations(Partition partition) {
    List<String> nodes = new ArrayList<>();
    nodes.addAll(((HHRDDPartition) partition).getPreferredHosts());
    if (nodes == null || nodes.isEmpty()) {
      return null;
    }
    return scala.collection.JavaConversions.asScalaBuffer(nodes).seq();
  }

  /**
   * Creates the temp dir.
   *
   * @return the file
   */
  public static File createTempDir() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    String baseName = System.currentTimeMillis() + "-";

    for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
      File tempDir = new File(baseDir, baseName + counter);
      if (tempDir.mkdir()) {
        return tempDir;
      }
    }
    throw new IllegalStateException("Failed to create directory within "
            + TEMP_DIR_ATTEMPTS + " attempts (tried "
            + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
  }
}
