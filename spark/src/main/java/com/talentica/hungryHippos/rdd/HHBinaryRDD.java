/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;

import scala.collection.Iterator;
import scala.collection.mutable.Seq;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * This RDD is build for the binary storage file system.
 * 
 * @author pooshans
 *
 */
public class HHBinaryRDD extends HHRDD<byte[]> implements Serializable {
  private static final long serialVersionUID = -7855737619208668440L;
  private static final ClassTag<byte[]> HHRD_READER__TAG =
      ClassManifestFactory$.MODULE$.fromClass(byte[].class);

  public HHBinaryRDD(JavaSparkContext sc, HHRDDInfo hhrddInfo, Integer[] jobDimensions,
      boolean requiresShuffle) {
    super(sc, hhrddInfo, jobDimensions, requiresShuffle, HHRD_READER__TAG);
  }

  public HHBinaryRDD(JavaSparkContext sc, HHRDDInfo hhrddInfo, boolean requiresShuffle) {
    this(sc, hhrddInfo, ArrayUtils.toObject(hhrddInfo.getShardingIndexes()), requiresShuffle);
  }

  @Override
  public Iterator<byte[]> compute(Partition partition, TaskContext taskContext) {
    HHRDDPartition hhRDDPartion = (HHRDDPartition) partition;
    HHBinaryRDDIterator iterator = null;
    try {
      iterator = new HHBinaryRDDIterator(hhRDDPartion.getFilePath(), hhRDDPartion.getRowSize(),
          hhRDDPartion.getFiles(), hhRDDPartion.getNodIdToIp());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return iterator;
  }

  @Override
  public Partition[] getPartitions() {
    return this.partitions;
  }

  @Override
  public Seq<String> getPreferredLocations(Partition partition) {
    List<String> nodes = new ArrayList<>();
    nodes.addAll(((HHRDDPartition) partition).getPreferredHosts());
    if (nodes == null || nodes.isEmpty()) {
      return null;
    }
    return scala.collection.JavaConversions.asScalaBuffer(nodes).seq();
  }


}
