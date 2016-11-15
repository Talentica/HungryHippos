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
      files = getFiles();
      partitions = new Partition[files.size()];
      for (int index = 0; index < partitions.length; index++) {
        partitions[index] =
            new HungryHipposRDDPartition(index, files.get(index), hipposRDDConf.getRowSize());
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    return partitions;
  }

  private List<DataInputStream> getFiles() throws FileNotFoundException {
    List<DataInputStream> filesInputStream = new ArrayList<DataInputStream>();
    List<String> totalCombinations = new ArrayList<String>();
    for (int index = 0; index < hipposRDDConf.getShardingIndexes().length; index++) {
      for (int bucket = 0; bucket < hipposRDDConf.getBuckets(); bucket++) {
        String name = totalCombinations.get(index);
        if (name == null || "".equals(name)) {
          name = "" + bucket;
        } else if (index != hipposRDDConf.getShardingIndexes().length - 1) {
          name = name + "_" + bucket;
        } else {
          name = name + bucket;
        }
        totalCombinations.add(index, name);
      }
    }
    for (String fileName : totalCombinations) {
      filesInputStream.add(new DataInputStream(new FileInputStream(new File(fileName))));
    }
    totalCombinations.clear();
    return filesInputStream;
  }

}
