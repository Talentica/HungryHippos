package com.talentica.hungryHippos.sharding.main;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;

/**
 * {@code ShardingStarter} used for creating the sharding table.
 * 
 *
 */
public class ShardingStarter {

  private static ClientConfig clientConfig;
  /**
   * @param args
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStarter.class);
  private static ShardingApplicationContext context;

  public static void main(String[] args) {
    LOGGER.info("SHARDING PROCESS STARTED");
    boolean isFileCreated = false;
    String shardingTablePathOnZk = null;
    HungryHippoCurator curator = null;
    try {
      LOGGER.info("SHARDING STARTED");
      long startTime = System.currentTimeMillis();
      validateArguments(args);

      String clientConfigFilePath = args[0];
      String shardingFolderPath = args[1];
      float bucketCountWeight = 0.5f;
      if (args.length > 2) {
        bucketCountWeight = Float.parseFloat(args[2]);
      }

      clientConfig = JaxbUtil.unmarshalFromFile(clientConfigFilePath, ClientConfig.class);
      int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
      String connectString = clientConfig.getCoordinationServers().getServers();
      curator = HungryHippoCurator.getInstance(connectString, sessionTimeOut);
      context = new ShardingApplicationContext(shardingFolderPath);

      ShardingClientConfig shardingClientConfig = context.getShardingClientConfig();

      String distributedFilePath = shardingClientConfig.getInput().getDistributedFilePath();
      FileSystemUtils.validatePath(distributedFilePath, true);
      if (distributedFilePath == null || "".equals(distributedFilePath)) {
        throw new RuntimeException("Distributed File path cannot be empty");
      }
      shardingTablePathOnZk =
          ShardingApplicationContext.getShardingConfigFilePathOnZk(distributedFilePath);
      boolean fileAlreadyExists = curator.checkExists(shardingTablePathOnZk);
      if (fileAlreadyExists) {
        throw new RuntimeException(shardingTablePathOnZk + " already exists");
      }
      curator.createPersistentNode(shardingTablePathOnZk, FileSystemConstants.IS_A_FILE);

      isFileCreated = true;

      String tempDir = FileUtils.getUserDirectoryPath() + File.separator + "temp" + File.separator
          + "hungryhippos" + File.separator + System.currentTimeMillis() + File.separator
          + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
      new File(tempDir).mkdirs();
      doSharding(shardingClientConfig, context.getShardingClientConfigFilePath(),
          context.getShardingServerConfigFilePath(), tempDir, bucketCountWeight);
      List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
      String uploadDestinationPath = getUploadDestinationPath(shardingClientConfig);
      uploadShardingData(nodes, shardingClientConfig, tempDir);
      curator.createPersistentNode(shardingTablePathOnZk + FileSystemConstants.ZK_PATH_SEPARATOR
          + FileSystemConstants.SHARDED, uploadDestinationPath);
      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to do sharding.", ((endTime - startTime) / 1000.0));
    } catch (Exception exception) {
      LOGGER.error("Error occurred while sharding.", exception);
      if (isFileCreated) {
        try {
          curator.delete(shardingTablePathOnZk);
        } catch (HungryHippoException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

  }

  private static void validateArguments(String[] args) {
    if (args.length < 2) {
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
      String shardingClientConfigFilePath, String shardingServerConfigFilePath, String tempDir,
      float bucketCountWeight) throws ClassNotFoundException, IOException, NoSuchMethodException,
      IllegalAccessException, InvocationTargetException, InstantiationException,
      InterruptedException, JAXBException, KeeperException {
    LOGGER.info("SHARDING STARTED");
    String sampleFilePath = shardingClientConfig.getInput().getSampleFilePath();
    String dataParserClassName =
        shardingClientConfig.getInput().getDataParserConfig().getClassName();
    DataDescription dataDescription = context.getConfiguredDataDescription();
    DataParser dataParser = (DataParser) Class.forName(dataParserClassName)
        .getConstructor(DataDescription.class).newInstance(dataDescription);

    ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
    Reader inputReaderForSharding = getInputReaderForSharding(sampleFilePath, dataParser);

    Sharding sharding = new Sharding(clusterConfig, context, bucketCountWeight);
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
   * @param nodes
   * @param shardingClientConfig
   * @param tempDir
   */
  private static void uploadShardingData(List<Node> nodes,
      ShardingClientConfig shardingClientConfig, String tempDir) {
    LOGGER.info("Uploading sharding data.");
    // Output outputConfiguration = clientConfig.getOutput();
    ShardingTableCopier shardingTableCopier =
        new ShardingTableCopier(tempDir, shardingClientConfig);
    shardingTableCopier.copyToAllNodeInCluster(nodes);
    LOGGER.info("Upload completed.");
  }

  /**
   * Returns the uploadDestination path of zipped sharding files
   * 
   * @param shardingClientConfig
   * @return
   */
  public static String getUploadDestinationPath(ShardingClientConfig shardingClientConfig) {
    String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
    String distributedFilePath = shardingClientConfig.getInput().getDistributedFilePath();
    String destinationDirectory = fileSystemBaseDirectory + distributedFilePath;
    String uploadDestinationPath =
        destinationDirectory + "/" + ShardingTableCopier.SHARDING_ZIP_FILE_NAME + ".tar.gz";
    return uploadDestinationPath;
  }

}
