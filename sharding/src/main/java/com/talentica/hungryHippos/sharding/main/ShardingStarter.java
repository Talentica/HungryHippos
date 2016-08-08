package com.talentica.hungryHippos.sharding.main;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
import com.talentica.hungryhippos.config.sharding.Output;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.config.sharding.ShardingServerConfig;

public class ShardingStarter {

  /**
   * @param args
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStarter.class);

  public static void main(String[] args) {
    try {
      LOGGER.info("SHARDING STARTED");
      long startTime = System.currentTimeMillis();
      validateArguments(args);
      String shardingClientConfigFilePath = args[1];
      ShardingClientConfig shardingClientConfig =
          JaxbUtil.unmarshalFromFile(shardingClientConfigFilePath, ShardingClientConfig.class);
      ShardingApplicationContext.setShardingClientConfig(
          shardingClientConfig.getInput().getDistributedFilePath(), shardingClientConfig);
      String shardingServerConfigFilePath = args[2];
      ShardingServerConfig shardingServerConfig =
          JaxbUtil.unmarshalFromFile(shardingServerConfigFilePath, ShardingServerConfig.class);
      ShardingApplicationContext.setShardingServerConfig(
          shardingClientConfig.getInput().getDistributedFilePath(), shardingServerConfig);
      String sampleFilePath = shardingClientConfig.getInput().getSampleFilePath();
      String dataParserClassName =
          shardingClientConfig.getInput().getDataParserConfig().getClassName();
      DataParser dataParser =
          (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
              .newInstance(ShardingApplicationContext.getConfiguredDataDescription(
                  shardingClientConfig.getInput().getDistributedFilePath()));

      String clientConfigFilePath = args[0];
      NodesManager manager = NodesManagerContext.getNodesManagerInstance(clientConfigFilePath);
      CoordinationConfig coordinationConfig =
          CoordinationApplicationContext.getZkCoordinationConfigCache();
      manager.initializeZookeeperDefaultConfig(coordinationConfig.getZookeeperDefaultConfig());

      ZKNodeFile clusterConfigFile = (ZKNodeFile) manager
          .getConfigFileFromZNode(CoordinationApplicationContext.CLUSTER_CONFIGURATION);
      ClusterConfig clusterConfig =
          JaxbUtil.unmarshal((String) clusterConfigFile.getObj(), ClusterConfig.class);
      Sharding sharding = new Sharding(clusterConfig);
      sharding.doSharding(getInputReaderForSharding(sampleFilePath, dataParser),
          shardingClientConfig.getInput().getDistributedFilePath());
      Output outputConfiguration = shardingClientConfig.getOutput();
      String tempDir = FileUtils.getUserDirectoryPath() + File.separator + "temp" + File.separator
          + "hungryhippos" + File.separator + System.currentTimeMillis();
      new File(tempDir).mkdirs();
      sharding.dumpShardingTableFiles(tempDir, shardingClientConfigFilePath,
          shardingServerConfigFilePath);
      ShardingTableCopier shardingTableCopier =
          new ShardingTableCopier(tempDir, shardingClientConfig, outputConfiguration);
      shardingTableCopier.copyToAnyRandomNodeInCluster();
      long endTime = System.currentTimeMillis();
      LOGGER.info("SHARDING DONE!!");
      LOGGER.info("It took {} seconds of time to do sharding.", ((endTime - startTime) / 1000));
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

}
