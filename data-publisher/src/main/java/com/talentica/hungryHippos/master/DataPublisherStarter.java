/**
 * 
 */
package com.talentica.hungryHippos.master;

import com.talentica.hungryHippos.coordination.DataSyncCoordinator;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.Chunk;
import com.talentica.hungryHippos.utility.FileSplitter;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.HHFStream;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryHippos.utility.StopWatch;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@code DataPublisherStarter} responsible for call the {@code DataProvider} which publish data to
 * the node.
 */
public class DataPublisherStarter {

  private static HungryHippoCurator curator;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);
  private static String userName = null;
  private static String SCRIPT_FOR_FILE_SPLIT = "hh-split-file.sh";
  private static String destinationPathNode;
  private static String pathForSuccessNode;
  private static String pathForFailureNode;
  private static String pathForClientUploadsInParallelNode;
  private static String pathForClientUploadNode;

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
      List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
      int noOfChunks = getNoOfChunks(args, nodes);
      File srcFile = new File(sourcePath);
      updateZookeeperNodes(destinationPath);
      String remotePath = FileSystemContext.getRootDirectory() + destinationPath + File.separator
          + UUID.randomUUID().toString();
      long startTime = System.currentTimeMillis();
      Map<Integer, DataInputStream> dataInputStreamMap = new ConcurrentHashMap<>();
      Map<Integer, Socket> socketMap = new ConcurrentHashMap<>();
      List<Chunk> chunks = null;

      FileSplitter fileSplitter = null;

      if (noOfChunks == 1) {

        fileSplitter = new FileSplitter(sourcePath, noOfChunks);
        chunks = fileSplitter.start();
        Chunk chunk = chunks.get(0);

        uploadChunk(destinationPath, nodes, remotePath, dataInputStreamMap, socketMap, chunk, 0);
      } else {

        fileSplitter = new FileSplitter(sourcePath, noOfChunks);
        chunks = fileSplitter.start();

        int noOfParallelThreads = getNoOfParallelThreads(noOfChunks, srcFile);
        ExecutorService executorService = Executors.newFixedThreadPool(noOfParallelThreads);
        ChunkUpload[] chunkUpload = new ChunkUpload[noOfChunks];
        for (Chunk chunk : chunks) {

          chunkUpload[chunk.getId()] = new ChunkUpload(chunk, nodes, destinationPath, remotePath,
              dataInputStreamMap, socketMap);
          executorService.execute(chunkUpload[chunk.getId()]);
        }


        executorService.shutdown();
        while (!executorService.isTerminated()) {

        }
        boolean success = true;
        for (int i = 0; i < noOfChunks; i++) {
          success = success && chunkUpload[i].isSuccess();
        }
        if (!success) {
          throw new RuntimeException("File Publish failed");
        }
      }

      boolean success = true;
      for (int i = 0; i < noOfChunks; i++) {
        LOGGER.info("Waiting for status of chunk : {}", i + 1);
        String status = dataInputStreamMap.get(i).readUTF();
        LOGGER.info("status of chunk {}/{} is {} ", i + 1, noOfChunks, status);
        if (!HungryHippoServicesConstants.SUCCESS.equals(status)) {
          success = false;
        }
        socketMap.get(i).close();
      }
      if (!success) {
        throw new RuntimeException("File Publish Failed for " + destinationPath);
      }

      updateSuccessFul();

      long endTime = System.currentTimeMillis();
      LOGGER.info("Data Publish Successful");
      LOGGER.info("It took {} seconds of time to for publishing.", ((endTime - startTime) / 1000));
    } catch (Exception exception) {
      exception.printStackTrace();
      LOGGER.error("Error occured while executing publishing data on nodes.", exception);
      updateFilePublishFailure();
    }
  }

  private static int getNoOfParallelThreads(int noOfChunks, File srcFile) {
    long oneGB = 1073741824;
    long srcFileSize = srcFile.length();
    long diskUsableSpace = srcFile.getUsableSpace();
    long approxSizeOfChunk = (srcFileSize + oneGB) / noOfChunks;
    long maxNoOfParallelTransfer = diskUsableSpace / approxSizeOfChunk;
    int optimumNoOfThreads = Runtime.getRuntime().availableProcessors() * 2;
    int noOfParallelThreads;
    if (maxNoOfParallelTransfer >= noOfChunks) {
      noOfParallelThreads = noOfChunks > optimumNoOfThreads ? optimumNoOfThreads : noOfChunks;
    } else {
      noOfParallelThreads = maxNoOfParallelTransfer > optimumNoOfThreads ? optimumNoOfThreads
          : (int) maxNoOfParallelTransfer;
    }
    LOGGER.info("No Of Parallel Threads for chunk upload : {}", noOfParallelThreads);
    if (noOfParallelThreads < 1) {
      noOfParallelThreads = 1;
    }
    return noOfParallelThreads;
  }

  private static int getNoOfChunks(String[] args, List<Node> nodes) {
    int noOfChunks;
    if (args.length > 3) {
      noOfChunks = Integer.parseInt(args[3]);
    } else {
      noOfChunks = nodes.size();
    }
    return noOfChunks;
  }

  private static void updateZookeeperNodes(String destinationPath) throws HungryHippoException {
    destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + destinationPath;
    pathForSuccessNode = destinationPathNode + "/" + FileSystemConstants.DATA_READY;
    pathForFailureNode = destinationPathNode + "/" + FileSystemConstants.PUBLISH_FAILED;
    pathForClientUploadsInParallelNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
        + FileSystemConstants.CLIENT_UPLOADS_IN_PARALLEL;
    curator.deletePersistentNodeIfExits(pathForSuccessNode);
    curator.deletePersistentNodeIfExits(pathForFailureNode);
    curator.createPersistentNodeIfNotPresent(pathForClientUploadsInParallelNode);
    int clientId =
        createClientNode(pathForClientUploadsInParallelNode + HungryHippoCurator.ZK_PATH_SEPERATOR);
    pathForClientUploadNode =
        pathForClientUploadsInParallelNode + HungryHippoCurator.ZK_PATH_SEPERATOR + clientId;
  }

  private static int createClientNode(String path) {
    int i = 0;
    while (true) {
      try {
        curator.createPersistentNode(path + i);
        return i;
      } catch (HungryHippoException e) {
        if (e instanceof HungryHippoException) {
          i++;
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }


  public static void uploadChunk(String destinationPath, List<Node> nodes, String remotePath,
      Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Chunk chunk,
      int nodeId) throws IOException, InterruptedException {
    Node node = nodes.get(nodeId);
    // transferChunk(remotePath, chunkFilePath, node);
    requestDataDistribution(destinationPath, remotePath, dataInputStreamMap, socketMap, chunk,
        node);
  }

  private static void requestDataDistribution(String destinationPath, String remotePath,
      Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Chunk chunk,
      Node node) throws IOException, InterruptedException {
    Socket socket = ServerUtils.connectToServer(node.getIp() + ":" + 8789, 10);
    dataInputStreamMap.put(chunk.getId(), new DataInputStream(socket.getInputStream()));
    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
    dos.writeInt(HungryHippoServicesConstants.DATA_DISTRIBUTOR);
    dos.writeUTF(destinationPath);
    dos.writeUTF(remotePath + File.separator + chunk.getFileName());
    dos.writeLong(chunk.getActualSizeOfChunk());
    dos.flush();
    int size = socket.getSendBufferSize();
    byte[] buffer = new byte[size];
    HHFStream hhfsStream = chunk.getHHFStream();
    int read = 0;
    StopWatch stopWatch = new StopWatch();
    LOGGER.info("Started data Transfer of chunk {} ", chunk.getId());
    while ((read = hhfsStream.read(buffer)) != -1) {
      dos.write(buffer, 0, read);
    }
    dos.flush();
    hhfsStream.close();
    LOGGER.info("Finished transferring chunk {} , it took {} seconds", chunk.getId(),
        stopWatch.elapsedTime());
    // socket.shutdownOutput();

    if (dataInputStreamMap.get(chunk.getId()).readUTF()
        .equals(HungryHippoServicesConstants.SUCCESS)) {
      LOGGER.info("chunk with {} id reached the node {} id successfully ", chunk.getId(),
          node.getIdentifier());
    } else {
      throw new RuntimeException("Chunk was not successfully uploaded");
    }
    socketMap.put(chunk.getId(), socket);

  }

  /*
   * private static void transferChunk(String remotePath, String chunkFilePath, Node node) throws
   * IOException, InterruptedException { Socket socket = ServerUtils.connectToServer(node.getIp() +
   * ":" + 8789, 10); DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
   * DataInputStream dis = new DataInputStream(socket.getInputStream());
   * dos.writeInt(HungryHippoServicesConstants.SCP_ACCESS); dos.flush(); String status =
   * dis.readUTF(); if (!HungryHippoServicesConstants.SUCCESS.equals(status)) { throw new
   * RuntimeException("File Scp not possible"); } ScpCommandExecutor.upload(userName, node.getIp(),
   * remotePath, chunkFilePath); dos.writeBoolean(true); socket.close(); }
   */

  private static void validateArguments(String[] args) {
    if (args.length < 3) {
      throw new RuntimeException(
          "Either missing 1st argument {client configuration} or 2nd argument {source path} or 3rd argument {destination path}");
    }
  }


  /**
   * Updates file pubising failed
   *
   *
   */
  private static void updateFilePublishFailure() {
    try {
      curator.deletePersistentNodeIfExits(pathForSuccessNode);
      curator.createPersistentNodeIfNotPresent(pathForFailureNode);
      curator.deletePersistentNodeIfExits(pathForClientUploadNode);
    } catch (HungryHippoException e) {
      LOGGER.error(e.getMessage());
    }
  }

  public static void updateSuccessFul() throws HungryHippoException {
    if (!checkIfFailed()) {
      curator.deletePersistentNodeIfExits(pathForClientUploadNode);
      List children = curator.getChildren(pathForClientUploadsInParallelNode);
      if (children == null || children.isEmpty()) {
        curator.deletePersistentNodeIfExits(pathForClientUploadsInParallelNode);
        curator.createPersistentNodeIfNotPresent(pathForSuccessNode);
      }

    }
  }

  public static boolean checkIfFailed() throws HungryHippoException {
    return curator.checkExists(pathForFailureNode);
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
