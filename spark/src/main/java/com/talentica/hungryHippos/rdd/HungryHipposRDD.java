/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * @author pooshans
 *
 */
public class HungryHipposRDD extends RDD<ByteBuffer> {

  private static final long serialVersionUID = -1546634848854956364L;
  private static final ClassTag<ByteBuffer> BYTE_BUFFER_TAG =
      ClassManifestFactory$.MODULE$.fromClass(ByteBuffer.class);
  private HungryHipposRDDConf hipposRDDConf;

  public HungryHipposRDD(SparkContext sc, HungryHipposRDDConf hipposRDDConf) {
    super(sc, new ArrayBuffer<Dependency<?>>(), BYTE_BUFFER_TAG);
    this.hipposRDDConf = hipposRDDConf;
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
    List<DataInputStream> files;
    Partition[] partitions = null;
    try {
      files = getFiles(hipposRDDConf.getDirectoryLocation());
      partitions = new Partition[files.size()];
      for (int index = 0; index < partitions.length; index++) {
        partitions[index] =
            new HungryHipposRDDPartition(index, files.get(index), hipposRDDConf.getRowSize());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return partitions;
  }

  private List<DataInputStream> getFiles(String dataDirectory) throws IOException {
    List<DataInputStream> filesInputStream = new ArrayList<DataInputStream>();
    File[] files = new File(dataDirectory).listFiles();
    for (File file : files) {
      DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file));
      if(dataInputStream.available() > 0 ){
        filesInputStream.add(dataInputStream);
      }
    }
    return filesInputStream;
  }

}
