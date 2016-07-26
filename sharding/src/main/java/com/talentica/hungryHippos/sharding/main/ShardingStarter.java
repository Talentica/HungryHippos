package com.talentica.hungryHippos.sharding.main;

import java.io.IOException;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.sharding.ShardingTableZkService;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
import com.talentica.hungryhippos.config.sharding.ShardingConfig;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;

public class ShardingStarter {

	private static String outPutDir = null;

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStarter.class);

	public static void main(String[] args) {
		try {
			validateArguments(args);
			LOGGER.info("SHARDING STARTED");
			long startTime = System.currentTimeMillis();
			setContext(args);
			ShardingConfig shardingConfig = ShardingApplicationContext.getZkShardingConfigCache();
			String sampleFilePath = shardingConfig.getInput().getSampleFilePath();
			LOGGER.debug("SampleFilePath is " + sampleFilePath);
			String inputFilePath = shardingConfig.getInputFileConfig().getInputFileName();
			LOGGER.debug("inputFilePath is " + inputFilePath);
			String dataParserClassName = shardingConfig.getInput().getDataParserConfig().getClassName();
			CoordinationConfig coordinationConfig = CoordinationApplicationContext.getZkCoordinationConfigCache();
			DataParser dataParser = (DataParser) Class.forName(dataParserClassName)
					.getConstructor(DataDescription.class)
					.newInstance(CoordinationApplicationContext.getConfiguredDataDescription());
			Sharding sharding = new Sharding(coordinationConfig.getClusterConfig());
			sharding.doSharding(getInputReaderForSharding(sampleFilePath, dataParser));
			outPutDir = shardingConfig.getOutput().getOutputDir();
			sharding.dumpShardingTableFiles(outPutDir);
			long endTime = System.currentTimeMillis();
			LOGGER.info("SHARDING DONE!!");
			LOGGER.info("It took {} seconds of time to do sharding.", ((endTime - startTime) / 1000));
			HungryHipposFileSystem hhfs = HungryHipposFileSystem.getInstance();
			hhfs.createZnode(inputFilePath);
			ShardingTableZkService shardingTable = new ShardingTableZkService();
			LOGGER.info("Uploading started for sharded table bucketCombinationToNodeNumbersMap...");
		//	shardingTable.zkUploadBucketCombinationToNodeNumbersMap();
			LOGGER.info("Completed.");
			long start = Calendar.getInstance().getTimeInMillis();
			LOGGER.info("Uploading started for sharded table bucketToNodeNumberMap...");
			shardingTable.zkUploadBucketToNodeNumberMap();
			LOGGER.info("Completed.");
			LOGGER.info("Uploading started for sharded table keyToValueToBucketMap...");
			shardingTable.zkUploadKeyToValueToBucketMap();
			int queuedEvents = 0;
			while (true) {
				Thread.sleep(60000);
				queuedEvents = ZkUtils.getQueuedEvents();
				LOGGER.info("Queued Events :- " + queuedEvents);
				if (queuedEvents == 0) {
					break;
				}
				
			}

			long end = Calendar.getInstance().getTimeInMillis();
			LOGGER.info( "Completed after " + (end - start)/1000 +  "seconds ");
		} catch (Exception exception) {
			LOGGER.error("Error occurred while sharding.", exception);
		}
	}

	public static String getOutPutDir() {
		return outPutDir;
	}

	private static void validateArguments(String[] args) {
		if (args.length < 1) {
			throw new RuntimeException("Missing zookeeper xml configuration file path arguments.");
		}
	}

	private static Reader getInputReaderForSharding(String inputFile, DataParser dataParser) throws IOException {
		return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(inputFile, dataParser);
	}

	private static void setContext(String[] args) {
		NodesManagerContext.setZookeeperXmlPath(args[0]);
	}

}
