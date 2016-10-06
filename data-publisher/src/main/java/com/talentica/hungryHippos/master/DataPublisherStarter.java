/**
 * 
 */
package com.talentica.hungryHippos.master;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.DataSyncCoordinator;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.RandomNodePicker;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;

public class DataPublisherStarter {

  private static HungryHippoCurator curator;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);
  private static ShardingApplicationContext context;
  private static String userName = null;

  public static void main(String[] args) {

    validateArguments(args);
    String clientConfigFilePath = args[0];
    String sourcePath = args[1];
    String destinationPath = args[2];

    try {
      ClientConfig clientConfig =
          JaxbUtil.unmarshalFromFile(clientConfigFilePath, ClientConfig.class);
      String connectString = clientConfig.getCoordinationServers().getServers();
      int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
      userName = clientConfig.getOutput().getNodeSshUsername();
      curator = HungryHippoCurator.getInstance(connectString, sessionTimeOut);
      FileSystemUtils.validatePath(destinationPath, true);

      String shardingZipRemotePath = getShardingZipRemotePath(destinationPath);
      checkFilesInSync(shardingZipRemotePath);
      String localShardingPath = FileUtils.getUserDirectoryPath() + File.separator + "temp"
          + File.separator + "hungryhippos" + File.separator + System.currentTimeMillis();
      new File(localShardingPath).mkdirs();
      localShardingPath = downloadAndUnzipShardingTable(shardingZipRemotePath, localShardingPath);
      LOGGER.info("Sharding local path {}", localShardingPath);
      context = new ShardingApplicationContext(localShardingPath);
      String dataParserClassName =
          context.getShardingClientConfig().getInput().getDataParserConfig().getClassName();
      DataParser dataParser =
          (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
              .newInstance(context.getConfiguredDataDescription());
      LOGGER.info("Initializing nodes manager.");
      long startTime = System.currentTimeMillis();
      DataProvider.publishDataToNodes(dataParser, sourcePath, destinationPath);
      updateFilePublishSuccessful(destinationPath);
      
      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to for publishing.", ((endTime - startTime) / 1000));
    } catch (Exception exception) {
      updateFilePublishFailure(destinationPath);
      LOGGER.error("Error occured while executing publishing data on nodes.", exception);
      exception.printStackTrace();
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 3) {
      throw new RuntimeException(
          "Either missing 1st argument {client configuration} or 2nd argument {source path} or 3rd argument {destination path}");
    }
  }

  /**
   * 
   */
  private static void dataPublishingFailed() {
    CountDownLatch signal = new CountDownLatch(1);
    String alertPathForDataPublisherFailure = curator
        .buildAlertPathByName(CommonUtil.ZKJobNodeEnum.DATA_PUBLISHING_FAILED.getZKJobNode());

    try {
      DataPublisherStarter.curator.createPersistentNode(alertPathForDataPublisherFailure);

    } catch (HungryHippoException e) {
      LOGGER.info("Unable to create the sharding failure path");
    }
    createErrorEncounterSignal();
  }

  private static void createErrorEncounterSignal() {
    LOGGER.info("ERROR_ENCOUNTERED signal is sent");
    String alertErrorEncounterDataPublisher = DataPublisherStarter.curator
        .buildAlertPathByName(CommonUtil.ZKJobNodeEnum.ERROR_ENCOUNTERED.getZKJobNode());
    try {
      DataPublisherStarter.curator.createPersistentNode(alertErrorEncounterDataPublisher);

    } catch (HungryHippoException e) {
      LOGGER.info("Unable to create the sharding failure path");
    }
  }


  /**
   * Updates file published successfully
   * 
   * @param destinationPath
   */
  private static void updateFilePublishSuccessful(String destinationPath) {
    String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + destinationPath;
    String pathForSuccessNode = destinationPathNode + "/" + FileSystemConstants.DATA_READY;
    String pathForFailureNode = destinationPathNode + "/" + FileSystemConstants.PUBLISH_FAILED;

    try {
      curator.deletePersistentNodeIfExits(pathForFailureNode);
      curator.createPersistentNodeIfNotPresent(pathForSuccessNode);
    } catch (HungryHippoException e) {
      LOGGER.error("creation of File update failed" + e.getLocalizedMessage());
    }

  }
  
  /**
   * Updates file pubising failed
   * 
   * @param destinationPath
   */
  private static void updateFilePublishFailure(String destinationPath) {
    String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + destinationPath;
    String pathForSuccessNode = destinationPathNode + "/" + FileSystemConstants.DATA_READY;
    String pathForFailureNode = destinationPathNode + "/" + FileSystemConstants.PUBLISH_FAILED;

    try {
      curator.deletePersistentNodeIfExits(pathForSuccessNode);
      curator.createPersistentNodeIfNotPresent(pathForFailureNode);
    } catch (HungryHippoException e) {
      LOGGER.error(e.getMessage());
    }
  }

  public static ShardingApplicationContext getContext() {
    return context;
  }

  /**
   * Downloads sharding zip from remote and unzips it in local local and returns the path of the
   * local directory
   * 
   * @param shardingZipRemotePath
   * @param localDir
   * @return
   */
  public static String downloadAndUnzipShardingTable(String shardingZipRemotePath,
      String localDir) {
    Node node = RandomNodePicker.getRandomNode();
    String filePath = localDir + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
    ScpCommandExecutor.download(userName, node.getIp(), shardingZipRemotePath, localDir);
    try {
      TarAndGzip.untarTGzFile(filePath + ".tar.gz");
    } catch (IOException e) {
      LOGGER.error("Downloading The sharding file from node : " + node.getIp() + " failed.");
      throw new RuntimeException(e);
    }
    return filePath;
  }

  /**
   * Returns Sharding Zip Path in remote
   * 
   * @param distributedFilePath
   * @return
   */
  public static String getShardingZipRemotePath(String distributedFilePath) {
    String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
    String remoteDir = fileSystemBaseDirectory + distributedFilePath;
    String shardingZipRemotePath =
        remoteDir + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME + ".tar.gz";
    return shardingZipRemotePath;
  }

  public static void checkFilesInSync(String shardingZipRemotePath) throws Exception {
    boolean shardingFileInSync = DataSyncCoordinator.checkSyncUpStatus(shardingZipRemotePath);
    if (!shardingFileInSync) {
      LOGGER.error("Sharding file {} not in sync", shardingZipRemotePath);
      throw new RuntimeException("Sharding file not in sync");
    }
    LOGGER.info("Sharding file {} in sync", shardingZipRemotePath);
  }
}
