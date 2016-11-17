package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.io.IOException;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * @author pooshans
 *
 */
public class HungryHipposRDD extends RDD<HHRDDRowReader> {

  private static final long serialVersionUID = -1546634848854956364L;
  private static final ClassTag<HHRDDRowReader> HHRD_READER__TAG =
      ClassManifestFactory$.MODULE$.fromClass(HHRDDRowReader.class);
  private HungryHipposRDDConf hipposRDDConf;

  public HungryHipposRDD(SparkContext sc, HungryHipposRDDConf hipposRDDConf) {
    super(sc, new ArrayBuffer<Dependency<?>>(), HHRD_READER__TAG);
    this.hipposRDDConf = hipposRDDConf;
  }

  @Override
  public Iterator<HHRDDRowReader> compute(Partition partition, TaskContext taskContext) {
    HungryHipposRDDPartition hhRDDPartion = (HungryHipposRDDPartition) partition;
    HungryHipposRDDIterator iterator = null;
    try {
      iterator = new HungryHipposRDDIterator(hhRDDPartion);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return iterator;
  }

  @Override
  public Partition[] getPartitions() {
    File[] files = new File(hipposRDDConf.getDirectoryLocation()).listFiles();
    Partition[] partitions = new Partition[files.length];
    for (int index = 0; index < partitions.length; index++) {
      partitions[index] = new HungryHipposRDDPartition(index, files[index].getPath(),
          hipposRDDConf.getDataDescription());
    }
    return partitions;
  }
}
