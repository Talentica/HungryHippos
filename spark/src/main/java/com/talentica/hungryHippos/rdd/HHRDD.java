package com.talentica.hungryHippos.rdd;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * @author pooshans
 *
 */
public class HHRDD extends RDD<byte[]> implements Serializable {
  private static final long serialVersionUID = 4074885953480955556L;
  private static final ClassTag<byte[]> HHRD_READER__TAG =
      ClassManifestFactory$.MODULE$.fromClass(byte[].class);
  private HHRDDConfigSerialized hipposRDDConf;
  private List<SerializedNode> nodesSer;
  private int id;
  private Partition[] partitions;

  public HHRDD(JavaSparkContext sc, HHRDDConfigSerialized hipposRDDConf) {
    super(sc.sc(), new ArrayBuffer<Dependency<?>>(), HHRD_READER__TAG);
    this.hipposRDDConf = hipposRDDConf;
    HHRDDHelper.populateBucketCombinationToNodeNumber(hipposRDDConf);
    HHRDDHelper.populateBucketToNodeNumber(hipposRDDConf);
    this.nodesSer = hipposRDDConf.getNodes();
    this.id = sc.sc().newRddId();
    this.partitions = HHRDDHelper.getPartition(hipposRDDConf, id);
  }

  @Override
  public Iterator<byte[]> compute(Partition partition, TaskContext taskContext) {
    HHRDDPartition hhRDDPartion = (HHRDDPartition) partition;
    HHRDDIterator iterator = null;
    try {
      iterator = new HHRDDIterator(hhRDDPartion.getFilePath(), hhRDDPartion.getRowSize(),hhRDDPartion.getHosts());
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
    List<String> nodes =
        HHRDDHelper.getPreferedIpsFromSetOfNode(partition, nodesSer, hipposRDDConf.getJobPrimDim());
    if (nodes == null) {
      return null;
    }
    return scala.collection.JavaConversions.asScalaBuffer(nodes).seq();
  }
}
