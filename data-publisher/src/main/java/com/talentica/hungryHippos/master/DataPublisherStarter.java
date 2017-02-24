/**
 * 
 */
package com.talentica.hungryHippos.master;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.*;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import org.eclipse.jetty.util.ArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
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

  private static String destinationPathNode;
  private static String pathForSuccessNode;
  private static String pathForFailureNode;
  private static String pathForClientUploadsInParallelNode;
  private static String pathForClientUploadNode;
  static Map<Integer, Queue<Node>> listOfNodesAssignedToThread = new HashMap<>();
  static Queue<Chunk> queue;

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
    long sizeOfChunk = 128 * 1024 * 1024;// 128MB
    if (args.length > 3) {
      sizeOfChunk = Long.parseLong(args[3]);
    }

    try {
      ClientConfig clientConfig =
          JaxbUtil.unmarshalFromFile(clientConfigFilePath, ClientConfig.class);
      String connectString = clientConfig.getCoordinationServers().getServers();
      int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
      curator = HungryHippoCurator.getInstance(connectString, sessionTimeOut);
      FileSystemUtils.validatePath(destinationPath, true);
      String shardingZipRemotePath = getShardingZipRemotePath(destinationPath);
      List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
      File srcFile = new File(sourcePath);
      updateZookeeperNodes(destinationPath);
      String remotePath = FileSystemContext.getRootDirectory() + destinationPath + File.separator;
      long startTime = System.currentTimeMillis();
      Map<Integer, DataInputStream> dataInputStreamMap = new ConcurrentHashMap<>();
      Map<Integer, Socket> socketMap = new ConcurrentHashMap<>();
      List<Chunk> chunks = null;


      FileSplitter fileSplitter = new FileSplitter(sourcePath, sizeOfChunk);

      int noOfChunks = fileSplitter.getNumberOfchunks();

      chunks = fileSplitter.start();

      int noOfParallelThreads = getNoOfParallelThreads(noOfChunks, srcFile);
      ExecutorService executorService = Executors.newFixedThreadPool(noOfParallelThreads);
      ChunkUpload[] chunkUpload = new ChunkUpload[noOfParallelThreads];


      for (int i = 0; i < noOfParallelThreads; i++) {
        listOfNodesAssignedToThread.put(i, new ArrayQueue<>(nodes.size()));
      }

      for (int i = 0; i < nodes.size(); i++) {
        int threadNo = i % noOfParallelThreads;
        listOfNodesAssignedToThread.get(threadNo).add(nodes.get(i));
      }
      if(nodes.size() < noOfParallelThreads){
        for (int i = nodes.size(); i < noOfParallelThreads; i++) {
          listOfNodesAssignedToThread.get(i).addAll(nodes);
        }
      }

      queue = new ArrayBlockingQueue<>(chunks.size());
      queue.addAll(chunks);

      for (int i = 0; i < noOfParallelThreads; i++) {
        if (!listOfNodesAssignedToThread.get(i).isEmpty()) {
          chunkUpload[i] = new ChunkUpload(destinationPath, remotePath, dataInputStreamMap,
              socketMap, listOfNodesAssignedToThread.get(i));
          executorService.execute(chunkUpload[i]);
        }
      }

      executorService.shutdown();
      while (!executorService.isTerminated()) {

      }

      boolean success = true;
      for (int i = 0; i < noOfParallelThreads; i++) {
        if(!listOfNodesAssignedToThread.get(i).isEmpty()) {
          success = success && chunkUpload[i].isSuccess();
        }
      }
      if (!success) {
        throw new RuntimeException("File Publish failed");
      }


      success = true;
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
      updateMetaData(destinationPath, nodes);
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
    long diskUsableSpace = srcFile.getUsableSpace();
    long approxSizeOfChunk = 134217728;
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

  public static void updateMetaData(String hhFilePath, List<Node> nodes)
      throws IOException, InterruptedException {
    for (int i = 0; i < nodes.size(); i++) {
      updateNodeMetaData(hhFilePath, nodes.get(i));
    }
  }

  public static void updateNodeMetaData(String hhFilePath, Node node)
      throws IOException, InterruptedException {
    Socket socket = ServerUtils.connectToServer(node.getIp() + ":" + node.getPort(), 10);
    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
    DataInputStream dis = new DataInputStream(socket.getInputStream());
    dos.writeInt(HungryHippoServicesConstants.METADATA_SYNCHRONIZER);
    dos.writeUTF(hhFilePath);
    String status = dis.readUTF();
    if (!HungryHippoServicesConstants.SUCCESS.equals(status)) {
      throw new RuntimeException("Metadata Synchronizer update failed for ip : " + node.getIp());
    }
  }

  public static void uploadChunk(String destinationPath, Queue<Node> nodes, String remotePath,
      Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap)
      throws IOException, InterruptedException {

    boolean successfulUpload = false;
    Node peekNode = nodes.peek();
    while (!successfulUpload) {
      Node node = nodes.poll();
      successfulUpload =
          requestDataDistribution(destinationPath, remotePath, dataInputStreamMap, socketMap, node);
      while (!nodes.offer(node));
      if(peekNode==nodes.peek()){
        Thread.sleep(10000);
      }
    }

  }

  private static boolean requestDataDistribution(String destinationPath, String remotePath,
      Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Node node)
      throws IOException, InterruptedException {
    Chunk chunk = null;
    String uuid;
    synchronized (queue) {
      chunk = queue.poll();
      uuid = UUID.randomUUID().toString();
    }

    if (chunk == null) {
      return true;
    }
    Socket socket = ServerUtils.connectToServer(node.getIp() + ":" + node.getPort(), 50);
    DataInputStream dis = new DataInputStream(socket.getInputStream());

    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
    dos.writeInt(HungryHippoServicesConstants.DATA_DISTRIBUTOR);
    dos.flush();


    boolean dataDistributorAvailable ;
    try{
      dataDistributorAvailable = dis.readBoolean();
    }catch (EOFException e){
      e.printStackTrace();
      dataDistributorAvailable = false;
    }
    LOGGER.info("[{}] DataDistributor Available in {} : {}", Thread.currentThread().getName(),
        node.getIp(), dataDistributorAvailable);
    if (dataDistributorAvailable) {
      dataInputStreamMap.put(chunk.getId(), dis);
      dos.writeUTF(destinationPath);
      dos.writeUTF(remotePath + uuid + File.separator + chunk.getFileName());
      dos.writeLong(chunk.getActualSizeOfChunk());
      dos.flush();
      int size = socket.getSendBufferSize();
      byte[] buffer = new byte[size];
      HHFStream hhfsStream = chunk.getHHFStream();
      int read = 0;
      StopWatch stopWatch = new StopWatch();
      LOGGER.info("Started data Transfer of chunk {} ", chunk.getId());
      LOGGER.info("Chunk size of chunk id {} is {} ", chunk.getId(), chunk.getActualSizeOfChunk());
      while ((read = hhfsStream.read(buffer)) != -1) {
        dos.write(buffer, 0, read);
      }
      dos.flush();
      hhfsStream.close();
      LOGGER.info("Finished transferring chunk {} , it took {} seconds", chunk.getId(),
          stopWatch.elapsedTime());

      if (dataInputStreamMap.get(chunk.getId()).readUTF()
          .equals(HungryHippoServicesConstants.SUCCESS)) {
        LOGGER.info("chunk with {} id reached the node {} id successfully ", chunk.getId(),
            node.getIdentifier());
      } else {
        throw new RuntimeException("Chunk was not successfully uploaded");
      }
      socketMap.put(chunk.getId(), socket);
    } else {
      while (!queue.offer(chunk));
      socket.close();

    }
    return dataDistributorAvailable;

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
      List<String> children = curator.getChildren(pathForClientUploadsInParallelNode);
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


}
