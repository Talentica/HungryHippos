package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.MapUtils;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

/**
 * Sharding table printer utility to print sharding table for specified HH file on console.
 * 
 * @author nitink
 *
 */
public class ShardingTablePrinter {

  public static void main(String[] args) throws FileNotFoundException, JAXBException {

    validateArguments(args);
    String shardingTablePath = args[0];
    System.out.println("###### Key to value to bucket number map ######");
    System.out.println("\t" + "Key" + "\t" + "Value" + "\t" + "Bucket"
        + MapUtils.getFormattedString(ShardingFileUtil.readFromFileKeyToValueToBucket(
            shardingTablePath + File.separatorChar + "keyToValueToBucketMap")));
    System.out.println();
    System.out.println("###### Bucket combination to node numbers map ######");
    System.out.println("\t" + "BucketCombination" + "\t\t\t" + "Node"
        + MapUtils.getFormattedString(ShardingFileUtil.readFromFileBucketCombinationToNodeNumber(
            shardingTablePath + File.separatorChar + "bucketCombinationToNodeNumbersMap")));
    System.out.println();
    System.out.println("###### Bucket to node numbers map ######");
    System.out.println("\t" + "Bucket" + "\t\t\t" + "Node"
        + MapUtils.getFormattedString(ShardingFileUtil.readFromFileBucketToNodeNumber(
            shardingTablePath + File.separatorChar + "bucketToNodeNumberMap")));
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      System.err.println(
          "Please provide sharding-folder path parameters. e.g. java -cp sharding.jar com.talentica.hungryHippos.sharding.util.ShardingTablePrinter <sharding-folder>");
      System.exit(1);
    }
  }

}
