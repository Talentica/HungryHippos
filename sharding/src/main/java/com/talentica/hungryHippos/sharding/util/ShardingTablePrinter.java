package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.sharding.ShardingTableZkService;
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

    NodesManager manager = NodesManagerContext.getNodesManagerInstance(args[0]);
    CoordinationConfig coordinationConfig =
        CoordinationApplicationContext.getZkCoordinationConfigCache();
    manager.initializeZookeeperDefaultConfig(coordinationConfig.getZookeeperDefaultConfig());
    String shardingTablePath = args[1];
    // ShardingTableZkService service = new ShardingTableZkService();
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
    ClusterConfig clusterConfig = CoordinationApplicationContext.getZkClusterConfigCache();
    System.out.println("##### Node details ####");
    System.out.println();
    System.out.println("Node Id" + "\t\t" + "Node name" + "\t\t" + "Address");
    clusterConfig.getNode().forEach(node -> System.out
        .println(node.getIdentifier() + "\t\t" + node.getName() + "\t\t" + node.getIp()));
  }

  private static void validateArguments(String[] args) {
    if (args.length < 2) {
      System.err.println(
          "Please provide with client configuration xml file path and hungry hippos file path parameters. e.g. java -cp sharding.jar com.talentica.hungryHippos.sharding.util.ShardingTablePrinter client-config.xml /myfolder/youtube_data.csv");
      System.exit(1);
    }
  }

}
