/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;

import scala.collection.Iterator;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * This RDD is build on the text {@code HungryHipposFileSystem}.
 * @author pooshans
 *
 */
public class HHTextRDD extends HHRDD<String> implements Serializable {

  private static final ClassTag<String> HHRD_READER__TAG =
      ClassManifestFactory$.MODULE$.fromClass(String.class);
  private static final long serialVersionUID = 4074885953480955556L;
  private HHRDDInfo hhrddInfo;

  public HHTextRDD(JavaSparkContext sc, HHRDDInfo hhrddInfo, Integer[] jobDimensions,
      boolean requiresShuffle) {
    super(sc, hhrddInfo, jobDimensions, requiresShuffle, HHRD_READER__TAG);
    this.hhrddInfo = hhrddInfo;
  }

  public HHTextRDD(JavaSparkContext sc, HHRDDInfo hhrddInfo, boolean requiresShuffle) {
    this(sc, hhrddInfo, ArrayUtils.toObject(hhrddInfo.getShardingIndexes()), requiresShuffle);
  }

  @Override
  public Iterator<String> compute(Partition partition, TaskContext taskContext) {
    HHRDDPartition hhRDDPartion = (HHRDDPartition) partition;
    HHTextRDDIterator iterator = null;
    try {
      iterator = new HHTextRDDIterator(hhRDDPartion.getFilePath(), hhRDDPartion.getFiles(),
          hhRDDPartion.getNodeInfo(),hhrddInfo.getAbsoluteDataFilePath());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return iterator;
  }

}
