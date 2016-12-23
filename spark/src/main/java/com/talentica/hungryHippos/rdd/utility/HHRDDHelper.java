package com.talentica.hungryHippos.rdd.utility;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.HHRDDPartition;
import com.talentica.hungryHippos.rdd.SerializedNode;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;

public class HHRDDHelper implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -3090261268542999403L;
  private static Logger logger = LoggerFactory.getLogger(HHRDDHelper.class);
  public final static String bucketCombinationToNodeNumbersMapFile =
      "bucketCombinationToNodeNumbersMap";
  public static Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap = null;
  private static HashMap<Integer, List<String>> cachePreferedLocation =
      new HashMap<Integer, List<String>>();
  private static Partition[] partitions = null;
  private static HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;
  public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";
  private static String[]  keyOrder;

  public static List<String> getPreferedIpsFromSetOfNode(Partition partition,
      List<SerializedNode> nodesSer, int primDim) {
    HHRDDPartition hhrddPartition = ((HHRDDPartition) partition);
    String fileName = hhrddPartition.getFileName();
    Integer partitionId = hhrddPartition.index();
    List<String> nodes = cachePreferedLocation.get(partitionId);
    if (nodes != null && !nodes.isEmpty()) {
      return nodes;
    }
    // BucketCombination{{key1=Bucket{4}, key2=Bucket{0}, key3=Bucket{8}}
    int hashCode = getHashCode(fileName);
    Set<Node> nodeSet = null;
    for (Entry<BucketCombination, Set<Node>> entry : bucketCombinationToNodeNumberMap.entrySet()) {
      if (entry.getKey().toString().hashCode() == hashCode) {
        nodeSet = entry.getValue();
        break;
      }
    }

    if (nodeSet == null) {
      logger.warn("Partition  {} does not exists", fileName);
      return null;
    }

    List<String> ips = selectPreferedNodesIp(nodeSet, nodesSer);
    hhrddPartition.setHosts(ips);
    nodes = getPreferredIp(ips, primDim);
    cachePreferedLocation.put(partitionId, nodes);
    return nodes;
  }

  private static int getHashCode(String fileName) {
    String key = "BucketCombination{{";
    String[] bucketIds = fileName.split("_");
    for (int i = 0; i < bucketIds.length; i++) {
      if (i < (bucketIds.length - 1)) {
        key = key + "key" + (i + 1) + "=Bucket{" + bucketIds[i] + "}, ";
      } else {
        key = key + "key" + (i + 1) + "=Bucket{" + bucketIds[i] + "}}}";
      }
    }
    return key.hashCode();
  }


  public static void populateBucketCombinationToNodeNumber(HHRDDConfigSerialized hipposRDDConf) {
    String bucketCombinationToNodeNumbersMapFilePath = hipposRDDConf.getShardingFolderPath()
        + File.separatorChar + bucketCombinationToNodeNumbersMapFile;

    if (bucketCombinationToNodeNumberMap == null) {
      bucketCombinationToNodeNumberMap = ShardingFileUtil
          .readFromFileBucketCombinationToNodeNumber(bucketCombinationToNodeNumbersMapFilePath);
    }
  }

  public static void populateBucketToNodeNumber(HHRDDConfigSerialized hipposRDDConf) {
    String bucketToNodeNumberMapFilePath =
        hipposRDDConf.getShardingFolderPath() + File.separatorChar + bucketToNodeNumberMapFile;
    if (bucketToNodeNumberMap == null) {
      bucketToNodeNumberMap =
          ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFilePath);
    }
  }

  public static String[] getFiles(String dataDirectory) throws IOException {
    String[] files = new File(dataDirectory).list(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        logger.debug("Directory is {} , name is {} ", dir, name);
        try {
          if (Files.size(Paths.get(dir + "" + File.separatorChar + name)) == 0) {
            return false;
          }
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
        return true;
      }
    });
    return files;
  }

  public static int getPrimaryDimensionIndexToRunJobWith(Job job, int[] sortedShardingIndexes) {
    Integer[] jobDimensions = job.getDimensions();
    List<Integer> dimensionsList = new ArrayList<>();
    Arrays.stream(jobDimensions).forEach(value -> dimensionsList.add(value));
    int[] filteredPrimaryOnlyJobDimensions = Arrays.stream(sortedShardingIndexes)
        .filter(shardIndex -> dimensionsList.contains(shardIndex)).toArray();
    return getShardingIndexForJobExecutionToMaximizeUseOfSortedData(
        filteredPrimaryOnlyJobDimensions);
  }

  public static Partition[] getPartition(HHRDDConfigSerialized hipposRDDConf, int id) {
    if (partitions == null) {
      List<String> fileNames = new ArrayList<>();
      keyOrder= hipposRDDConf.getShardingKeyOrder();
      listFile(fileNames, "", 0, keyOrder.length);
      partitions = new HHRDDPartition[fileNames.size()];
      for (int index = 0; index < fileNames.size(); index++) {
        String filePathAndName =
            hipposRDDConf.getDirectoryLocation() + File.separatorChar + fileNames.get(index);
        partitions[index] = new HHRDDPartition(id, index, new File(filePathAndName).getPath(),
            hipposRDDConf.getFieldTypeArrayDataDescription());
      }
    }
    return partitions;

  }

  private static void listFile(List<String> fileNames, String fileName, int dim,   int shardDim) {
    if (dim == shardDim) {
      fileNames.add(fileName);
      return;
    }
    for (int i = 0; i < bucketToNodeNumberMap.get(keyOrder[dim]).size(); i++) {
      if (dim == 0) {
        listFile(fileNames, i + fileName, dim + 1,shardDim);
      } else {
        listFile(fileNames, fileName + "_" + i, dim + 1, shardDim);
      }
    }

  }
  
/* public static void main(String[] args) {
   String shardingTablePath = "/home/pooshans/HungryHippos/HungryHippos/sharding-table";
   ShardingApplicationContext context = new ShardingApplicationContext(shardingTablePath);
   bucketToNodeNumberMap = ShardingFileUtil.readFromFileBucketToNodeNumber(
       shardingTablePath + File.separatorChar + "bucketToNodeNumberMap");
   System.out.println(bucketToNodeNumberMap);
   List<String> fileNames = new ArrayList<>();
   listFile(fileNames, "", 0, keyOrder.length);
   System.out.println(fileNames);
}*/

  private static int getShardingIndexForJobExecutionToMaximizeUseOfSortedData(
      int[] primaryOnlyJobDimensions) {
    Arrays.sort(primaryOnlyJobDimensions);
    int maxPrimaryJobDimension = primaryOnlyJobDimensions[primaryOnlyJobDimensions.length - 1];
    int[] dimensions = new int[maxPrimaryJobDimension + 1];
    Arrays.stream(primaryOnlyJobDimensions)
        .forEach(currentDimension -> dimensions[currentDimension] = 1);
    int start = -1;
    int lastSum = 0;
    int currentSum = 0;
    int tempStart = -1;
    int initialSum = 0;

    for (int j = 0; j < dimensions.length; j++) {
      if (dimensions[j] == 1) {
        if (tempStart == -1) {
          tempStart = j;
        }
        currentSum++;
      } else {
        if (dimensions[0] == 1 && initialSum == 0) {
          initialSum = currentSum;
        }
        currentSum = 0;
        tempStart = -1;
      }

      if (currentSum >= lastSum) {
        start = tempStart;
        lastSum = currentSum;
      }

    }
    if (dimensions[0] == 1 && dimensions[dimensions.length - 1] == 1
        && (currentSum + initialSum) >= lastSum) {
      start = tempStart;
    }
    return start;
  }

  private static List<String> selectPreferedNodesIp(Set<Node> nodes,
      List<SerializedNode> nodesSer) {
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

  private static List<String> getPreferredIp(List<String> preferedNodesIp, int jobPrimDim) {
    List<String> preferredIp = new ArrayList<>();
    preferredIp.add(preferedNodesIp.get(jobPrimDim));
    return preferredIp;
  }

}
