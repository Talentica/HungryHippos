package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
  private HHRDDConfig hipposRDDConf;
  private List<SerializedNode> nodes;

  public HHRDD(SparkContext sc, HHRDDConfig hipposRDDConf) {
    super(sc, new ArrayBuffer<Dependency<?>>(), HHRD_READER__TAG);
    this.hipposRDDConf = hipposRDDConf;
    HHRDDHelper.populateBucketCombinationToNodeNumber(hipposRDDConf);
    nodes = hipposRDDConf.getNodes();
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
    String[] files;

    Partition[] partitions = null;
    try {
      files = HHRDDHelper.getFiles(hipposRDDConf.getDirectoryLocation());
      partitions = new Partition[files.length];
      for (int index = 0; index < partitions.length; index++) {
        String filePathAndName =
            hipposRDDConf.getDirectoryLocation() + File.separatorChar + files[index];
        partitions[index] = new HHRDDPartition(index, new File(filePathAndName).getPath(),
            hipposRDDConf.getFieldTypeArrayDataDescription());
      }
    } catch (IOException e) {
    }
    return partitions;
  }

  @Override
  public Seq<String> getPreferredLocations(Partition partition) {
    int nodeId = HHRDDHelper.getFirstIpFromSetOfNode(partition);
    if(nodeId == -1) return null;
    String nodeIp = null;
    for (SerializedNode node : nodes) {
      if (node.getId() == nodeId) {
        nodeIp = node.getIp();
        break;
      }
    }
    return new Some<String>(nodeIp).toList();

  }
}
