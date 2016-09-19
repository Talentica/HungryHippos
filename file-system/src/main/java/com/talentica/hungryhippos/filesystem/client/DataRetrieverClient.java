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
import java.util.ArrayList;
import java.util.List;

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
    boolean isSharded = ZkUtils.checkIfNodeExists(fsRootNode + hungryHippoFilePath
        + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.SHARDED);
    HungryHipposFileSystem.validateFileDataReady(hungryHippoFilePath);
    FileSystemUtils.createDirectory(outputDirName);
    for (String nodeId : nodeIds) {
      String nodeIdZKPath = fileNodeZKDFSPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
      String nodeIp = getNodeIp(nodeId);
      List<String> dataBlockNodes = ZkUtils.getChildren(nodeIdZKPath);
      if (isSharded) {
        downloadShardedFile(dataBlockNodes, dimension, nodeIdZKPath, hungryHippoFilePath,
            outputDirName, nodeIp, tmpDestFileNames);
      } else {
        downloadUnShardedFile(nodeIdZKPath, hungryHippoFilePath, outputDirName, nodeIp,
            tmpDestFileNames);
      }
    }
    combineFiles(tmpDestFileNames, hungryHippoFilePath, outputDirName);
  }

  /**
   * Returns IP of a node using nodeId
   *
   * @param nodeId
   * @return
   */
  public static String getNodeIp(String nodeId) {
    ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
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
   * Downloads a sharded files from a node
   *
   * @param dataBlockNodes
   * @param dimension
   * @param nodeIdZKPath
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
  public static void downloadShardedFile(List<String> dataBlockNodes, int dimension,
      String nodeIdZKPath, String hungryHippoFilePath, String outputDirName, String nodeIp,
      List<String> tmpDestFileNames) throws InterruptedException, ClassNotFoundException,
      IOException, KeeperException, JAXBException {
    int seq = 0;
    for (String dataBlockNode : dataBlockNodes) {
      int dataBlockIntVal = Integer.parseInt(dataBlockNode);
      int dimensionOperand = FileSystemUtils.getDimensionOperand(dimension);
      if ((dataBlockIntVal & dimensionOperand) == dimensionOperand) {
        String dataBlockZKPath =
            nodeIdZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + dataBlockNode;
        List<String> child = ZkUtils.getChildren(dataBlockZKPath);
        for (String chi : child) {
          String dataBlockSizeStr =
              (String) ZkUtils.getNodeData(dataBlockZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + chi);
          long dataSize = Long.parseLong(dataBlockSizeStr);
          String filePath = hungryHippoFilePath + FileSystemConstants.ZK_PATH_SEPARATOR
              + FileSystemContext.getDataFilePrefix() + dataBlockIntVal + FileSystemConstants.ZK_PATH_SEPARATOR + chi;
          String tmpDestFileName = outputDirName + FileSystemConstants.ZK_PATH_SEPARATOR + nodeIp
              + FileSystemConstants.DOWNLOAD_FILE_PREFIX
              + hungryHippoFilePath.replaceAll(FileSystemConstants.ZK_PATH_SEPARATOR, "-") + seq;
          retrieveDataBlocks(nodeIp, filePath, tmpDestFileName, dataSize);
          tmpDestFileNames.add(tmpDestFileName);
          seq++;
        }
      }
    }
  }

  /**
   * Downloads an Unsharded file from a node
   *
   * @param nodeIdZKPath
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
  public static void downloadUnShardedFile(String nodeIdZKPath, String hungryHippoFilePath,
      String outputDirName, String nodeIp, List<String> tmpDestFileNames)
      throws InterruptedException, ClassNotFoundException, IOException, KeeperException,
      JAXBException {
    String dataBlockSizeStr = (String) ZkUtils.getNodeData(nodeIdZKPath);
    long dataSize = Long.parseLong(dataBlockSizeStr);
    String tmpDestFileName = outputDirName + FileSystemConstants.ZK_PATH_SEPARATOR + nodeIp
        + FileSystemConstants.DOWNLOAD_FILE_PREFIX
        + hungryHippoFilePath.replaceAll(FileSystemConstants.ZK_PATH_SEPARATOR, "-");
    retrieveDataBlocks(nodeIp, hungryHippoFilePath, tmpDestFileName, dataSize);
    tmpDestFileNames.add(tmpDestFileName);
  }


  /**
   * Combines list of temporary files into single file
   * 
   * @param tmpDestFileNames
   * @param hungryHippoFilePath
   * @param outputDirName
   * @throws IOException
   */
  private static void combineFiles(List<String> tmpDestFileNames, String hungryHippoFilePath,
      String outputDirName) throws IOException {
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
   * @param hungryHippoFilePath
   * @param outputFile
   * @param dataSize
   */
  public static void retrieveDataBlocks(String nodeIp, String hungryHippoFilePath,
      String outputFile, long dataSize) throws InterruptedException, ClassNotFoundException,
      JAXBException, KeeperException, IOException {

    int port = FileSystemContext.getServerPort();
    int noOfAttempts = FileSystemContext.getMaxQueryAttempts();
    int fileStreamBufferSize = FileSystemContext.getFileStreamBufferSize();
    long retryTimeInterval = FileSystemContext.getQueryRetryInterval();
    retrieveDataBlocks(nodeIp, hungryHippoFilePath, outputFile, fileStreamBufferSize, dataSize,
        port, retryTimeInterval, noOfAttempts);
  }

  /**
   * This method is for requesting the DataRequestHandlerServer for the files
   *
   * @param nodeIp
   * @param hungryHippoFilePath
   * @param outputFile
   * @param dataSize
   * @param port
   * @param retryTimeInterval
   * @param maxQueryAttempts
   */
  public static void retrieveDataBlocks(String nodeIp, String hungryHippoFilePath,
      String outputFile, int fileStreamBufferSize, long dataSize, int port, long retryTimeInterval,
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
      } catch (InterruptedException e) {
        LOGGER.error(e.toString());
      }
    }
    if (i == maxQueryAttempts) {
      throw new RuntimeException("Data Retrieval Failed");
    }
  }

}
