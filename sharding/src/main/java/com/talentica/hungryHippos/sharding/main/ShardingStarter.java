package com.talentica.hungryHippos.sharding.main;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.sharding.Output;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;

public class ShardingStarter {

  /**
   * @param args
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStarter.class);

  public static void main(String[] args) {
    LOGGER.info("SHARDING PROCESS STARTED");
    boolean isFileCreated = false;
    String shardingTablePathOnZk = null;
    try {
      LOGGER.info("SHARDING STARTED");
      long startTime = System.currentTimeMillis();
      validateArguments(args);

      String clientConfigFilePath = args[0];
      String shardingClientConfigFilePath = args[1];
      String shardingServerConfigFilePath = args[2];

      NodesManagerContext.getNodesManagerInstance(clientConfigFilePath);
      ShardingApplicationContext.initialize(shardingClientConfigFilePath,
          shardingServerConfigFilePath);

      ShardingClientConfig shardingClientConfig =
          ShardingApplicationContext.getShardingClientConfig();

      String distributedFilePath = shardingClientConfig.getInput().getDistributedFilePath();
      if (distributedFilePath == null || "".equals(distributedFilePath)) {
        throw new RuntimeException("Distributed File path cannot be empty");
      }
      shardingTablePathOnZk =
          ShardingApplicationContext.getShardingConfigFilePathOnZk(distributedFilePath);
      boolean fileAlreadyExists = ZkUtils.checkIfNodeExists(shardingTablePathOnZk);
      if (fileAlreadyExists) {
        throw new RuntimeException(shardingTablePathOnZk + " already exists");
      }
      ZkUtils.createFileNode(shardingTablePathOnZk);
      isFileCreated = true;

      String tempDir = FileUtils.getUserDirectoryPath() + File.separator + "temp" + File.separator
          + "hungryhippos" + File.separator + System.currentTimeMillis();
      new File(tempDir).mkdirs();
      doSharding(shardingClientConfig, shardingClientConfigFilePath, shardingServerConfigFilePath,
          tempDir);
      uploadShardingData(shardingClientConfig, tempDir);
      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to do sharding.", ((endTime - startTime) / 1000));
    } catch (Exception exception) {
      LOGGER.error("Error occurred while sharding.", exception);
      if (isFileCreated) {
        ZkUtils.deleteZKNode(shardingTablePathOnZk);
      }
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      throw new RuntimeException("Missing zookeeper xml configuration file path arguments.");
    }
  }

  /**
   * Reads the configurations and starts sharding
   * 
   * @param shardingClientConfig
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws NoSuchMethodException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   * @throws InstantiationException
   * @throws InterruptedException
   * @throws JAXBException
   * @throws KeeperException
   */
  private static void doSharding(ShardingClientConfig shardingClientConfig,
      String shardingClientConfigFilePath, String shardingServerConfigFilePath, String tempDir)
      throws ClassNotFoundException, IOException, NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, InstantiationException, InterruptedException, JAXBException,
      KeeperException {
    LOGGER.info("SHARDING STARTED");
    String sampleFilePath = shardingClientConfig.getInput().getSampleFilePath();
    String dataParserClassName =
        shardingClientConfig.getInput().getDataParserConfig().getClassName();
    DataDescription dataDescription = ShardingApplicationContext.getConfiguredDataDescription();
    DataParser dataParser = (DataParser) Class.forName(dataParserClassName)
        .getConstructor(DataDescription.class).newInstance(dataDescription);

    ClusterConfig clusterConfig = CoordinationApplicationContext.getZkClusterConfigCache();
    Reader inputReaderForSharding = getInputReaderForSharding(sampleFilePath, dataParser);

    Sharding sharding = new Sharding(clusterConfig);
    sharding.doSharding(inputReaderForSharding);
    sharding.dumpShardingTableFiles(tempDir, shardingClientConfigFilePath,
        shardingServerConfigFilePath);
    LOGGER.info("SHARDING DONE!!");
  }

  private static Reader getInputReaderForSharding(String inputFile, DataParser dataParser)
      throws IOException {
    return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(inputFile,
        dataParser);
  }

  /**
   * Uploads Sharding related data to node
   * 
   * @param shardingTablePathOnZk
   * @param shardingClientConfigFilePath
   * @param shardingServerConfigFilePath
   */
  private static void uploadShardingData(ShardingClientConfig shardingClientConfig,
      String tempDir) {
    LOGGER.info("Uploading sharding data.");
    Output outputConfiguration = shardingClientConfig.getOutput();
    ShardingTableCopier shardingTableCopier =
        new ShardingTableCopier(tempDir, shardingClientConfig, outputConfiguration);
    shardingTableCopier.copyToAnyRandomNodeInCluster();
    LOGGER.info("Upload completed.");
  }


}
