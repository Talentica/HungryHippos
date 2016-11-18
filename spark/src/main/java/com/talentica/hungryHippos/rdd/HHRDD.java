package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

import scala.Some;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * @author pooshans
 *
 */
public class HHRDD extends RDD<HHRDDRowReader> {

  private static final long serialVersionUID = -1546634848854956364L;
  private static final ClassTag<HHRDDRowReader> HHRD_READER__TAG =
      ClassManifestFactory$.MODULE$.fromClass(HHRDDRowReader.class);
  private HHRDDConf hipposRDDConf;
  private Map<Integer, String> lookup = null;

  public HHRDD(SparkContext sc, HHRDDConf hipposRDDConf) {
    super(sc, new ArrayBuffer<Dependency<?>>(), HHRD_READER__TAG);
    this.hipposRDDConf = hipposRDDConf;
  }

  @Override
  public Iterator<HHRDDRowReader> compute(Partition partition, TaskContext taskContext) {
    HHRDDPartition hhRDDPartion = (HHRDDPartition) partition;
    HHRDDIterator iterator = null;
    try {
      iterator = new HHRDDIterator(hhRDDPartion);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return iterator;
  }

  @Override
  public Partition[] getPartitions() {
    List<File> files;

    Partition[] partitions = null;
    try {
      files = HHRDDHelper.getFiles(hipposRDDConf.getDirectoryLocation());

      partitions = new Partition[files.size()];
      for (int index = 0; index < partitions.length; index++) {
        partitions[index] = new HHRDDPartition(index, files.get(index).getPath(),
            hipposRDDConf.getDataDescription());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return partitions;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Seq<String> getPreferredLocations(Partition partition) {

    if (this.lookup == null) {
      this.lookup = this.hipposRDDConf.getNodeLookUp();
    }
    int nodeId = HHRDDHelper.getFirstIpFromSetOfNode(partition);
    String nodeIp = this.lookup.get(nodeId);
    return new Some(nodeIp).toList();

  }
}
