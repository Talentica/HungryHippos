package com.talentica.hungryHippos.rdd;

import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pooshans
 *
 */
public class HHRDD extends RDD<byte[]> implements Serializable {
  private static final long serialVersionUID = 4074885953480955556L;
  private static final ClassTag<byte[]> HHRD_READER__TAG =
      ClassManifestFactory$.MODULE$.fromClass(byte[].class);
  private int id;
  private Partition[] partitions;
  private HHRDDInfo hhrddInfo;

  public HHRDD(JavaSparkContext sc, HHRDDConfigSerialized hipposRDDConf,Integer[] jobDimensions) {
    super(sc.sc(), new ArrayBuffer<Dependency<?>>(), HHRD_READER__TAG);
    Map<Integer,String> nodIdToIp = new HashMap<>();
    for (SerializedNode serializedNode : hipposRDDConf.getNodes()){
      nodIdToIp.put(serializedNode.getId(),serializedNode.getIp());
    }
    this.hhrddInfo = new HHRDDInfo(HHRDDHelper.populateBucketCombinationToNodeNumber(hipposRDDConf),
            HHRDDHelper.populateBucketToNodeNumber(hipposRDDConf),hipposRDDConf.getShardingKeyOrder(),
            nodIdToIp);

    int[] shardingIndexes = hipposRDDConf.getShardingIndexes();
    List<Integer> jobShardingDimensions = new ArrayList<>();
    List<String> jobShardingDimensionsKey = new ArrayList<>();
    for (int i = 0; i < shardingIndexes.length; i++) {
      for (int j = 0; j < jobDimensions.length; j++) {
        if(shardingIndexes[i]==jobDimensions[j]){
          jobShardingDimensions.add(shardingIndexes[i]);
          jobShardingDimensionsKey.add(hipposRDDConf.getShardingKeyOrder()[i]);
        }
      }
    }

    if(jobShardingDimensions.isEmpty()){
      jobShardingDimensions.add(shardingIndexes[0]);
      jobShardingDimensionsKey.add(hipposRDDConf.getShardingKeyOrder()[0]);
    }
    this.id = sc.sc().newRddId();

    this.partitions = hhrddInfo.getPartition(hipposRDDConf, id,jobShardingDimensions, jobShardingDimensionsKey);
  }

  @Override
  public Iterator<byte[]> compute(Partition partition, TaskContext taskContext) {
    HHRDDPartition hhRDDPartion = (HHRDDPartition) partition;
    HHRDDIterator iterator = null;
    try {
      iterator = new HHRDDIterator(hhRDDPartion.getFilePath(), hhRDDPartion.getRowSize(),hhRDDPartion.getFiles(), hhRDDPartion.getNodIdToIp());
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
    nodes.add(((HHRDDPartition)partition).getPreferredHost());
    if (nodes == null||nodes.isEmpty()) {
      return null;
    }
    return scala.collection.JavaConversions.asScalaBuffer(nodes).seq();
  }
}
