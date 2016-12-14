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
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.sharding.BucketCombination;
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
  private static HashMap<Integer, Set<Node>> cachePreferedLocation =
      new HashMap<Integer, Set<Node>>();

  public static Set<Node> getPreferedIpsFromSetOfNode(Partition partition) {
    String fileName = ((HHRDDPartition) partition).getFileName();
    Integer partitionId = ((HHRDDPartition) partition).index();
    Set<Node> nodes = cachePreferedLocation.get(partitionId);
    if (nodes != null && !nodes.isEmpty()) {
      return nodes;
    }
    // BucketCombination{{key1=Bucket{4}, key2=Bucket{0}, key3=Bucket{8}}
    int hashCode = getHashCode(fileName);

    for (Entry<BucketCombination, Set<Node>> entry : bucketCombinationToNodeNumberMap.entrySet()) {
      if (entry.getKey().toString().hashCode() == hashCode) {
        nodes = entry.getValue();
        break;
      }
    }
    if (nodes == null)
      return null;
    logger.debug(" prefered locations for partition id {} whose file name is {}  is {}",
        partitionId, fileName, nodes.toString());
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

}
