package com.talentica.hungryhippos.filesystem.client;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * This class is for retrieving the sharded files from the HungryHippos Distributed File System
 * Created by rajkishoreh on 29/6/16.
 */
public class DataRetrieverClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataRetrieverClient.class);

  /**
   * This method is for reading the metadata from the zookeeper node and retrieving the data on a
   * default dimension 1
   * 
   * @param hungryHippoFilePath
   * @param outputDirName
   * @throws Exception
   */
  public static void getHungryHippoData(String hungryHippoFilePath, String outputDirName)
      throws Exception {
    getHungryHippoData(hungryHippoFilePath, outputDirName, 1);
  }

  /**
   * This method is for reading the metadata from the zookeeper node and retrieving the data on a
   * particular dimension
   *
   * @param hungryHippoFilePath
   * @param outputDirName
   * @param dimension
   * @throws Exception
   */
  public static void getHungryHippoData(String hungryHippoFilePath, String outputDirName,
      int dimension) throws Exception {
    List<String> tmpDestFileNames = new ArrayList<>();
    String fsRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath();
    String fileNodeZKDFSPath = fsRootNode + hungryHippoFilePath
        + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE;
    List<String> nodeIds = ZkUtils.getChildren(fileNodeZKDFSPath);
    Set<String> nodeIdSet = new HashSet<>();
    nodeIdSet.addAll(nodeIds);
    boolean isSharded = ZkUtils.checkIfNodeExists(fsRootNode + hungryHippoFilePath
        + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.SHARDED);
    HungryHipposFileSystem.validateFileDataReady(hungryHippoFilePath);
    FileSystemUtils.createDirectory(outputDirName);
    int dimensionOperand = FileSystemUtils.getDimensionOperand(dimension);
    for (String nodeId : nodeIdSet) {
      long dataFileSize = 0;
      String nodeIdZKPath = fileNodeZKDFSPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
      String nodeIp = getNodeIp(nodeId);
      if (isSharded) {
        downloadShardedFile(hungryHippoFilePath, outputDirName, tmpDestFileNames, nodeIdSet, dimensionOperand, nodeId, dataFileSize, nodeIdZKPath, nodeIp);
      } else {
        downloadUnShardedFile(nodeIdZKPath,Integer.parseInt(nodeId), hungryHippoFilePath, outputDirName,
            nodeIp, tmpDestFileNames);
      }
    }
    combineFiles(tmpDestFileNames,hungryHippoFilePath,outputDirName);
  }

  /**
   * Downloads a sharded files from a node
   *
   * @param hungryHippoFilePath
   * @param outputDirName
   * @param tmpDestFileNames
   * @param nodeIdSet
   * @param dimensionOperand
   * @param nodeId
   * @param dataFileSize
   * @param nodeIdZKPath
   * @param nodeIp
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws JAXBException
     * @throws KeeperException
     * @throws IOException
     */
  private static void downloadShardedFile(String hungryHippoFilePath, String outputDirName, List<String> tmpDestFileNames, Set<String> nodeIdSet,
                                          int dimensionOperand, String nodeId, long dataFileSize, String nodeIdZKPath, String nodeIp) throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
    List<String> dataFolderList = ZkUtils.getChildren(nodeIdZKPath);
    for(String dataFolder:dataFolderList){
      String dataFolderZKPath = nodeIdZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + dataFolder;
      int dataFolderIntVal = Integer.parseInt(dataFolder);
      if ((dataFolderIntVal & dimensionOperand) == dimensionOperand) {
        for (String dataFile : nodeIdSet) {
          String dataFileZKPath = dataFolderZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + dataFile;
          String dataFileSizeStr = (String) ZkUtils.getNodeData(dataFileZKPath);
          dataFileSize += Long.parseLong(dataFileSizeStr);
        }
      }
    }
    String tmpDestFileName = outputDirName + FileSystemConstants.ZK_PATH_SEPARATOR + nodeIp
            + hungryHippoFilePath.replaceAll(FileSystemConstants.ZK_PATH_SEPARATOR, "-");
    retrieveDataBlocks(nodeIp,Integer.parseInt(nodeId), hungryHippoFilePath, tmpDestFileName, dataFileSize , dimensionOperand);
    tmpDestFileNames.add(tmpDestFileName);
  }

  /**
   * Returns IP of a node using nodeId
   *
   * @param nodeId
   * @return
   */
  public static String getNodeIp(String nodeId) {
    ClusterConfig clusterConfig =
            CoordinationConfigUtil.getZkClusterConfigCache();
    String nodeIp = null;
    for (Node node : clusterConfig.getNode()) {
      if (node.getIdentifier() == Integer.parseInt(nodeId)) {
        nodeIp = node.getIp();
        break;
      }
    }
    if (nodeIp == null) {
      throw new RuntimeException("Cluster configuration not available for nodeId :" + nodeId);
    }
    return nodeIp;
  }


  /**
   * Downloads an Unsharded file from a node
   *
   * @param nodeIdZKPath
   * @param nodeId
   * @param hungryHippoFilePath
   * @param outputDirName
   * @param nodeIp
   * @param tmpDestFileNames
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws KeeperException
     * @throws JAXBException
     */
  public static void downloadUnShardedFile(String nodeIdZKPath, int nodeId, String hungryHippoFilePath, String outputDirName, String nodeIp,
                                           List<String> tmpDestFileNames) throws InterruptedException, ClassNotFoundException,
          IOException, KeeperException, JAXBException {
    String dataBlockSizeStr = (String) ZkUtils.getNodeData(nodeIdZKPath);
    long dataSize = Long.parseLong(dataBlockSizeStr);
    String tmpDestFileName = outputDirName + FileSystemConstants.ZK_PATH_SEPARATOR + nodeIp
            + FileSystemConstants.DOWNLOAD_FILE_PREFIX
            + hungryHippoFilePath.replaceAll(FileSystemConstants.ZK_PATH_SEPARATOR, "-");
    retrieveDataBlocks(nodeIp, nodeId, hungryHippoFilePath, tmpDestFileName, dataSize,-1);
    tmpDestFileNames.add(tmpDestFileName);
  }


  /**
   * Combines list of temporary files into single file
   * @param tmpDestFileNames
   * @param hungryHippoFilePath
   * @param outputDirName
   * @throws IOException
   */
  private static void combineFiles(List<String> tmpDestFileNames,String hungryHippoFilePath, String outputDirName ) throws IOException {
    if (!tmpDestFileNames.isEmpty()) {
      String fileName = (new File(hungryHippoFilePath)).getName();
      FileSystemUtils.combineFiles(tmpDestFileNames,
              outputDirName + FileSystemConstants.ZK_PATH_SEPARATOR + fileName);
      FileSystemUtils.deleteFiles(tmpDestFileNames);
    }
  }

  /**
   * This method forwards the request with configuration for retrieving data
   *
   * @param nodeIp
   * @param nodeId
   * @param hungryHippoFilePath
   * @param outputFile
   * @param dataSize
   * @param dimensionOperand
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws JAXBException
   * @throws KeeperException
     * @throws IOException
     */
  public static void retrieveDataBlocks(String nodeIp, int nodeId, String hungryHippoFilePath,
                                        String outputFile, long dataSize, int dimensionOperand) throws InterruptedException, ClassNotFoundException,
          JAXBException, KeeperException, IOException {

    int port = FileSystemContext.getServerPort();
    int noOfAttempts = FileSystemContext.getMaxQueryAttempts();
    int fileStreamBufferSize = FileSystemContext.getFileStreamBufferSize();
    long retryTimeInterval = FileSystemContext.getQueryRetryInterval();
    retrieveDataBlocks(nodeIp, nodeId, hungryHippoFilePath, outputFile, fileStreamBufferSize, dataSize, dimensionOperand,
            port, retryTimeInterval, noOfAttempts);
  }

  /**
   * This method is for requesting the DataRequestHandlerServer for the files
   *
   * @param nodeIp
   * @param nodeId
   * @param hungryHippoFilePath
   * @param outputFile
   * @param fileStreamBufferSize
   * @param dataSize
   * @param dimensionOperand
   * @param port
   * @param retryTimeInterval
   * @param maxQueryAttempts
   */
  public static void retrieveDataBlocks(String nodeIp, int nodeId, String hungryHippoFilePath,
      String outputFile, int fileStreamBufferSize, long dataSize,int dimensionOperand, int port, long retryTimeInterval,
      int maxQueryAttempts) {

    if (dataSize == (new File(outputFile)).length()) {
      return;
    } else if (dataSize < (new File(outputFile)).length()) {
      new File(outputFile).delete();
    }

    int i = 0;
    for (i = 0; i < maxQueryAttempts; i++) {
      try (Socket client = new Socket(nodeIp, port);
          DataOutputStream dos = new DataOutputStream(client.getOutputStream());
          DataInputStream dis = new DataInputStream(client.getInputStream());
          RandomAccessFile raf = new RandomAccessFile(outputFile, "rw");) {
        String processStatus = dis.readUTF();
        if (FileSystemConstants.DATA_SERVER_AVAILABLE.equals(processStatus)) {
          LOGGER.info("{} Recieving data into {}", processStatus, outputFile);
          dos.writeUTF(hungryHippoFilePath);
          dos.writeInt(dimensionOperand);
          dos.writeInt(nodeId);
          dos.writeLong(raf.length());
          raf.seek(raf.length());
          int len;
          byte[] inputBuffer = new byte[fileStreamBufferSize];
          long fileSize = dataSize - raf.length();
          while (fileSize > 0) {
            len = dis.read(inputBuffer);
            if (len < 0) {
              break;
            }
            raf.write(inputBuffer, 0, len);
            fileSize = fileSize - len;
          }
          if (fileSize > 0) {
            LOGGER.info("Download Incomplete. Retrying after {} milliseconds", retryTimeInterval);
            Thread.sleep(retryTimeInterval);
            continue;
          }
          LOGGER.info(dis.readUTF());
          break;
        } else {
          LOGGER.info("{} Retrying after {} milliseconds", processStatus, retryTimeInterval);
          Thread.sleep(retryTimeInterval);
        }
      } catch (IOException e) {
        LOGGER.error(e.toString());
        e.printStackTrace();
      } catch (InterruptedException e) {
        LOGGER.error(e.toString());
      }
    }
    if (i == maxQueryAttempts) {
      throw new RuntimeException("Data Retrieval Failed");
    }
  }

}
