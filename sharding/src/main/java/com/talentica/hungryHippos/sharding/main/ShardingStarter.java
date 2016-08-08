package com.talentica.hungryHippos.sharding.main;

import java.io.File;
import java.io.IOException;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.sharding.Output;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.config.sharding.ShardingServerConfig;

import javax.xml.bind.JAXBException;

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
      validateArguments(args);
      String shardingClientConfigFilePath = args[1];
      NodesManager manager = NodesManagerContext.getNodesManagerInstance(clientConfigFilePath);

      ShardingClientConfig shardingClientConfig = JaxbUtil.unmarshalFromFile(shardingClientConfigFilePath, ShardingClientConfig.class);

      String shardingServerConfigFilePath = args[2];
      if (distributedFilePath == null || "".equals(distributedFilePath)) {
        throw new RuntimeException("Distributed File path cannot be empty");
      }
      shardingTablePathOnZk = ShardingApplicationContext.getShardingConfigFilePathOnZk(distributedFilePath);
      boolean fileAlreadyExists = ZkUtils.checkIfNodeExists(shardingTablePathOnZk);
      if (fileAlreadyExists) {
        throw new RuntimeException(shardingTablePathOnZk + " already exists");
      }
      ZkUtils.createFileNode(shardingTablePathOnZk);
      isFileCreated = true;

      ShardingServerConfig shardingServerConfig = JaxbUtil.unmarshalFromFile(shardingServerConfigFilePath, ShardingServerConfig.class);
      ShardingApplicationContext.putShardingClientConfig(distributedFilePath, shardingClientConfig);
      ShardingApplicationContext.putShardingServerConfig(distributedFilePath, shardingServerConfig);

      doSharding(shardingClientConfig);
      uploadShardingData(shardingTablePathOnZk, shardingClientConfigFilePath, shardingServerConfigFilePath);
      LOGGER.info("SHARDING PROCESS COMPLETED SUCCESSFULLY");
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
  private static void doSharding(ShardingClientConfig shardingClientConfig) throws ClassNotFoundException, IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, InterruptedException, JAXBException, KeeperException {
    LOGGER.info("SHARDING STARTED");
    long startTime = System.currentTimeMillis();
    String distributedFilePath = shardingClientConfig.getInput().getDistributedFilePath();
    String sampleFilePath = shardingClientConfig.getInput().getSampleFilePath();
    String dataParserClassName =  shardingClientConfig.getInput().getDataParserConfig().getClassName();
    DataDescription dataDescription = ShardingApplicationContext.getConfiguredDataDescription(distributedFilePath);
    DataParser dataParser = (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
            .newInstance(dataDescription);

      String clientConfigFilePath = args[0];
    Reader inputReaderForSharding= getInputReaderForSharding(sampleFilePath,dataParser);
    String outputDirectory = shardingClientConfig.getOutput().getOutputDir();

    Sharding sharding = new Sharding(clusterConfig);
    sharding.doSharding(inputReaderForSharding,distributedFilePath);
    sharding.dumpShardingTableFiles(outputDirectory);
      Output outputConfiguration = shardingClientConfig.getOutput();
      String tempDir = FileUtils.getUserDirectoryPath() + File.separator + "temp" + File.separator
          + "hungryhippos" + File.separator + System.currentTimeMillis();
      new File(tempDir).mkdirs();
      sharding.dumpShardingTableFiles(tempDir, shardingClientConfigFilePath,
          shardingServerConfigFilePath);
      ShardingTableCopier shardingTableCopier =
          new ShardingTableCopier(tempDir, shardingClientConfig, outputConfiguration);
      shardingTableCopier.copyToAnyRandomNodeInCluster();
  }
   * Uploads Sharding related data to Zookeeper
   * @param shardingTablePathOnZk
   * @param shardingClientConfigFilePath
   * @param shardingServerConfigFilePath
     */
  private static void uploadShardingData(String shardingTablePathOnZk, String shardingClientConfigFilePath, String shardingServerConfigFilePath) {

    try {
      ShardingApplicationContext.uploadShardingConfigOnZk(shardingTablePathOnZk,
    } catch (IOException | InterruptedException | IllegalAccessException |JAXBException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }


}
