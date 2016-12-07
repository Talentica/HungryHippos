/**
 * 
 */
package com.talentica.hungryHippos.master;

import com.talentica.hungryHippos.coordination.DataSyncCoordinator;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.coordination.utility.RandomNodePicker;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * {@code DataPublisherStarter} responsible for call the {@code DataProvider} which publish data to
 * the node.
 * 
 *
 */
public class DataPublisherStarter {

  private static HungryHippoCurator curator;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);
  private static String userName = null;
  private static String SCRIPT_FOR_FILE_SPLIT = "hh-split-file.sh";

  /**
   * Entry point of execution.
   * 
   * @param args.
   */
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
      List<Node>  nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
      int noOfChunks = nodes.size();
      File srcFile = new File(sourcePath);
      String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
              .getZookeeperDefaultConfig().getFilesystemPath() + destinationPath;
      String pathForSuccessNode =
              destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR + FileSystemConstants.DATA_READY;
      String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
              + FileSystemConstants.PUBLISH_FAILED;
      curator.deletePersistentNodeIfExits(pathForSuccessNode);
      curator.deletePersistentNodeIfExits(pathForFailureNode);
      String remotePath = FileSystemContext.getRootDirectory()+destinationPath+File.separator+ UUID.randomUUID().toString();
      long startTime = System.currentTimeMillis();
      String chunkFilePathPrefix = sourcePath+"_"+startTime+"_";
      String chunkFileNamePrefix = srcFile.getName()+"_"+startTime+"_";
      Map<Integer, DataInputStream> dataInputStreamMap = new HashMap<>();
      Map<Integer, Socket> socketMap = new HashMap<>();
      String hungryHippoBinDir = System.getProperty("hh.bin.dir");
      if(hungryHippoBinDir==null){
        throw new RuntimeException("System property hh.bin.dir is not set. Set the property value to HungryHippo bin Directory");
      }
      String commonCommandArgs = hungryHippoBinDir+SCRIPT_FOR_FILE_SPLIT+" "+sourcePath
              +" "+noOfChunks;
      String line = "";
      int currentChunk = 0;
      for (int i = 0; i < noOfChunks; i++) {
        currentChunk = i+1;
        String chunkFilePath = chunkFilePathPrefix+i;
        Process process = Runtime.getRuntime().exec(commonCommandArgs+" "+currentChunk+" "+chunkFilePath);
        int processStatus = process.waitFor();
          if (processStatus != 0) {
            BufferedReader errReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((line = errReader.readLine()) != null) {
              LOGGER.error(line);
            }
            errReader.close();
            LOGGER.error("File publish failed for {}", destinationPath);
            throw new RuntimeException("File Publish failed");
          }
        File file = new File(chunkFilePath);
        file.deleteOnExit();
        ScpCommandExecutor.upload(userName,nodes.get(i).getIp(),remotePath,chunkFilePath);
        file.delete();
        Node node = nodes.get(i);
        Socket socket = ServerUtils.connectToServer(node.getIp()+":"+8789, 10);
        dataInputStreamMap.put(i, new DataInputStream(socket.getInputStream()));
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeUTF(destinationPath);
        dos.writeUTF(remotePath+File.separator+chunkFileNamePrefix+i);
        dos.flush();
        socketMap.put(i,socket);
      }

      for (int i = 0; i < noOfChunks; i++) {
        LOGGER.info("Waiting for status of chunk : {}",i+1);
        String status = dataInputStreamMap.get(i).readUTF();
        LOGGER.info("status of chunk {} is {} ",i+1,status);
        if("FAILED".equals(status)){
          throw new RuntimeException("File Publish Failed for "+destinationPath);
        }
        socketMap.get(i).close();
      }

      updateSuccessFul(destinationPath);

      long endTime = System.currentTimeMillis();
      LOGGER.info("Data Publish Successful");
      LOGGER.info("It took {} seconds of time to for publishing.", ((endTime - startTime) / 1000));
    } catch (Exception exception) {
      exception.printStackTrace();
      LOGGER.error("Error occured while executing publishing data on nodes.", exception);
      updateFilePublishFailure(destinationPath);
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 3) {
      throw new RuntimeException(
          "Either missing 1st argument {client configuration} or 2nd argument {source path} or 3rd argument {destination path}");
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

  public static void updateSuccessFul(String hhFilePath) throws HungryHippoException {
    if(!checkIfFailed(hhFilePath)){
      String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
              .getZookeeperDefaultConfig().getFilesystemPath() + hhFilePath;
      String pathForSuccessNode = destinationPathNode + "/" + FileSystemConstants.DATA_READY;
      curator.createPersistentNodeIfNotPresent(pathForSuccessNode);
    }
  }

  public static boolean checkIfFailed(String hhFilePath) throws HungryHippoException {
    HungryHippoCurator curator = HungryHippoCurator.getInstance();
    String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
            .getZookeeperDefaultConfig().getFilesystemPath() + hhFilePath;
    String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
            + FileSystemConstants.PUBLISH_FAILED;
    return  curator.checkExists(pathForFailureNode);
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
