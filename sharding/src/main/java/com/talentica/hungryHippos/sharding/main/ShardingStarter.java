package com.talentica.hungryHippos.sharding.main;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.sharding.ShardingTableZkService;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
import com.talentica.hungryhippos.config.sharding.ShardingConfig;

public class ShardingStarter {

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
      String dataParserClassName = shardingConfig.getInput().getDataParserConfig().getClassName();
      CoordinationConfig coordinationConfig =
          CoordinationApplicationContext.getZkCoordinationConfigCache();
      DataParser dataParser =
          (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
              .newInstance(CoordinationApplicationContext.getConfiguredDataDescription());
      Sharding sharding = new Sharding(coordinationConfig.getClusterConfig());
      sharding.doSharding(getInputReaderForSharding(sampleFilePath, dataParser));
      sharding.dumpShardingTableFiles(shardingConfig.getOutput().getOutputDir());
      long endTime = System.currentTimeMillis();
      LOGGER.info("SHARDING DONE!!");
      LOGGER.info("It took {} seconds of time to do sharding.", ((endTime - startTime) / 1000));
      ShardingTableZkService shardingTable = new ShardingTableZkService();
      LOGGER.info("Uploading started for sharded table bucketCombinationToNodeNumbersMap...");
      shardingTable.zkUploadBucketCombinationToNodeNumbersMap();
      LOGGER.info("Completed.");
      LOGGER.info("Uploading started for sharded table bucketToNodeNumberMap...");
      shardingTable.zkUploadBucketToNodeNumberMap();
      LOGGER.info("Completed.");
      LOGGER.info("Uploading started for sharded table keyToValueToBucketMap...");
      shardingTable.zkUploadKeyToValueToBucketMap();
      LOGGER.info("Completed.");
    } catch (Exception exception) {
      LOGGER.error("Error occurred while sharding.", exception);
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      throw new RuntimeException("Missing zookeeper xml configuration file path arguments.");
    }
  }

  private static Reader getInputReaderForSharding(String inputFile, DataParser dataParser)
      throws IOException {
    return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(inputFile,
        dataParser);
  }

  private static void setContext(String[] args) {
    NodesManagerContext.setZookeeperXmlPath(args[0]);
  }

}
