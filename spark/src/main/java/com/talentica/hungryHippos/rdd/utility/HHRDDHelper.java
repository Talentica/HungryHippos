package com.talentica.hungryHippos.rdd.utility;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.rdd.HHRDDConfig;
import com.talentica.hungryHippos.rdd.HHRDDPartition;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;

public class HHRDDHelper implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static Logger logger = LoggerFactory.getLogger(HHRDDHelper.class);
  public final static String bucketCombinationToNodeNumbersMapFile =
      "bucketCombinationToNodeNumbersMap";
  public static Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap = null;

  public static Set<Node>  getPreferedIpsFromSetOfNode(Partition partition) {
    String fileName = ((HHRDDPartition) partition).getFileName();
    // BucketCombination{{key1=Bucket{4}, key2=Bucket{0}, key3=Bucket{8}}
    String key = "BucketCombination{{";
    String[] bucketIds = fileName.split("_");
    for (int i = 0; i < bucketIds.length; i++) {
      if (i < (bucketIds.length - 1)) {
        key = key + "key" + (i + 1) + "=Bucket{" + bucketIds[i] + "},";
      } else {
        key = key + "key" + (i + 1) + "=Bucket{" + bucketIds[i] + "}}}";
      }
    }

    Set<Node> nodes = null;
    for (Entry<BucketCombination, Set<Node>> entry : bucketCombinationToNodeNumberMap.entrySet()) {
      logger.info(
          "Tabel bucket combination {} hashcode {} and generated bucket combination {} hashcode{}",
          entry.getKey(), entry.getKey().toString().hashCode(), key, key.hashCode());
      if (entry.getKey().toString().hashCode() == key.hashCode()) {
        nodes = entry.getValue();
        break;
      }
    }
    /*if (nodes == null) {
      logger.error("nodes are null");
      return -1;
    }*/
    /*List<Node> listNode = new ArrayList<>(nodes);

    int nodeId = listNode.get(0).getNodeId();*/

    logger.info(" prefered locations for partition index {} whose file name is {}  is {}",
        partition.index(), fileName, nodes.toString());
    return nodes;
  }


  public static void populateBucketCombinationToNodeNumber(HHRDDConfig hipposRDDConf) {
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
        logger.info("Directory is {} , name is {} ", dir, name);
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

}
