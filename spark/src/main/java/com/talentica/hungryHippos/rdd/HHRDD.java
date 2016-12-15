package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.hungryHippos.sharding.Node;

import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * @author pooshans
 *
 */
public class HHRDD extends RDD<HHRDDRowReader> implements Serializable {
  private static final long serialVersionUID = 4074885953480955556L;
  private static final ClassTag<HHRDDRowReader> HHRD_READER__TAG =
      ClassManifestFactory$.MODULE$.fromClass(HHRDDRowReader.class);
  private HHRDDConfigSerialized hipposRDDConf;
  private List<SerializedNode> nodesSer;
  private int id;

  public HHRDD(JavaSparkContext sc, HHRDDConfigSerialized hipposRDDConf) {
    super(sc.sc(), new ArrayBuffer<Dependency<?>>(), HHRD_READER__TAG);
    this.hipposRDDConf = hipposRDDConf;
    HHRDDHelper.populateBucketCombinationToNodeNumber(hipposRDDConf);
    nodesSer = hipposRDDConf.getNodes();
    this.id = sc.sc().newRddId();
  }

  @Override
  public Iterator<HHRDDRowReader> compute(Partition partition, TaskContext taskContext) {
    HHRDDPartition hhRDDPartion = (HHRDDPartition) partition;
    HHRDDIterator iterator = null;
    try {
      iterator = new HHRDDIterator(hhRDDPartion.getFilePath(), hhRDDPartion.getRowSize(),
          hhRDDPartion.getFieldTypeArrayDataDescription());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return iterator;
  }

  @Override
  public Partition[] getPartitions() {
    Partition[] partitions = null;
    List<String> fileNames = new ArrayList<>();
    listFile(fileNames, "", 0, hipposRDDConf.getMaxBuckets(),
        hipposRDDConf.getShardingIndexes().length);
    partitions = new HHRDDPartition[fileNames.size()];
    for (int index = 0; index < fileNames.size(); index++) {
      String filePathAndName =
          hipposRDDConf.getDirectoryLocation() + File.separatorChar + fileNames.get(index);
      partitions[index] = new HHRDDPartition(id, index, new File(filePathAndName).getPath(),
          hipposRDDConf.getFieldTypeArrayDataDescription());
    }
    return partitions;
  }

  @Override
  public Seq<String> getPreferredLocations(Partition partition) {
    Set<Node> nodes = HHRDDHelper.getPreferedIpsFromSetOfNode(partition);
    if (nodes == null) {
      return null;
    }
    List<String> preferedNodesIp =
        reorderPreferredIpsByJobPrimaryDimension(selectPreferedNodesIp(nodes));
    return scala.collection.JavaConversions.asScalaBuffer(preferedNodesIp).seq();
  }

  private List<String> selectPreferedNodesIp(Set<Node> nodes) {
    List<String> preferedNodesIp = new ArrayList<String>();
    for (SerializedNode nodeSer : nodesSer) {
      for (Node node : nodes) {
        if (nodeSer.getId() == node.getNodeId()) {
          preferedNodesIp.add(nodeSer.getIp());
          break;
        }
      }
    }
    return preferedNodesIp;
  }

  private List<String> reorderPreferredIpsByJobPrimaryDimension(List<String> preferedNodesIp) {
    List<String> reorderPreferedNodesIp = new ArrayList<>();
    int jobPrimDim = hipposRDDConf.getJobPrimDim();
    for (int i = 0; i < preferedNodesIp.size(); i++) {
      reorderPreferedNodesIp.add(i, preferedNodesIp.get((i + jobPrimDim) % preferedNodesIp.size()));
    }
    return reorderPreferedNodesIp;
  }

  private void listFile(List<String> fileNames, String fileName, int dim, int maxBucketSize,
      int shardDim) {
    if (dim == shardDim) {
      fileNames.add(fileName);
      return;
    }

    for (int i = 1; i <= maxBucketSize; i++) {
      if (dim == 0) {
        listFile(fileNames, i + fileName, dim + 1, maxBucketSize, shardDim);
      } else {
        listFile(fileNames, fileName + "_" + i, dim + 1, maxBucketSize, shardDim);
      }
    }

  }
}
