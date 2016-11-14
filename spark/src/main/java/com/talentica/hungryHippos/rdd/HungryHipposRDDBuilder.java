/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;

import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * @author pooshans
 *
 */
public abstract class HungryHipposRDDBuilder extends RDD<ByteBuffer> {

  private static final ClassTag<ByteBuffer> BYTE_BUFFER_TAG =
      ClassManifestFactory$.MODULE$.fromClass(ByteBuffer.class);
  private File[] dataFiles;
  private int rowSize;

  public HungryHipposRDDBuilder(SparkContext sc) {
    super(sc, new ArrayBuffer<Dependency<?>>(), BYTE_BUFFER_TAG);

  }

  @Override
  public Iterator<ByteBuffer> compute(Partition partition, TaskContext taskContext) {
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
    Partition[] partitions = new Partition[dataFiles.length];
    for (int index = 0; index < dataFiles.length; index++) {
      try {
        partitions[index] = new HungryHipposRDDPartition(index,
            new DataInputStream(new FileInputStream(dataFiles[index])), rowSize);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
    return partitions;
  }

  public void setConfiguration(File[] dataFiles, int rowSize) {
    this.dataFiles = dataFiles;
    this.rowSize = rowSize;
  }

}
