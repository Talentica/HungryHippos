package com.talentica.hungryHippos.sharding.util;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.sharding.ShardingTableZkService;
import com.talentica.hungryHippos.utility.MapUtils;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

/**
 * Sharding table printer utility to print sharding table for specified HH file
 * on console.
 * 
 * @author nitink
 *
 */
public class ShardingTablePrinter {

	public static void main(String[] args) {

		validateArguments(args);
		NodesManagerContext.setZookeeperXmlPath(args[0]);
		ShardingTableZkService service = new ShardingTableZkService();
		System.out.println("###### Key to value to bucket number map ######");
		System.out.println("\t" + "Key" + "\t" + "Value" + "\t" + "Bucket"
				+ MapUtils.getFormattedString(service.readKeyToValueToBucketMap()));
		System.out.println();
		System.out.println("###### Bucket combination to node numbers map ######");
		System.out.println("\t" + "BucketCombination" + "\t\t\t" + "Node"
				+ MapUtils.getFormattedString(service.readBucketCombinationToNodeNumbersMap()));
		System.out.println();
		CoordinationConfig coordinationConfig = CoordinationApplicationContext.getZkCoordinationConfigCache();
		System.out.println("##### Node details ####");
		System.out.println();
		System.out.println("Node Id" + "\t\t" + "Node name" + "\t\t" + "Address");
		coordinationConfig.getClusterConfig().getNode().forEach(
				node -> System.out.println(node.getIdentifier() + "\t\t" + node.getName() + "\t\t" + node.getIp()));
	}

	private static void validateArguments(String[] args) {
		if (args.length < 2) {
			System.err.println(
					"Please provide with client configuration xml file path and hungry hippos file path parameters. e.g. java -cp sharding.jar com.talentica.hungryHippos.sharding.util.ShardingTablePrinter client-config.xml /myfolder/youtube_data.csv");
			System.exit(1);
		}
	}

}
