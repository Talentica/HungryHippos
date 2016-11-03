package com.talentica.hungryhippos.filesystem.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.config.sharding.Column;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;


/**
 * {@code DataRetrieverClient } is for retrieving the sharded files from the HungryHippos
 * Distributed File System
 * 
 * @author rajkishoreh
 * @since 29/6/16.
 * @author sudarshans (updated).
 * 
 */
public class DataRetrieverClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataRetrieverClient.class);
  private static HungryHippoCurator curator = HungryHippoCurator.getInstance();

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
    HungryHippoCurator curator = HungryHippoCurator.getInstance();
    List<String> nodeIds = curator.getChildren(fileNodeZKDFSPath);
    boolean isSharded = curator.checkExists(fsRootNode + hungryHippoFilePath
        + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.SHARDED);

    Set<String> nodeIdSet = new HashSet<>();
    nodeIdSet.addAll(nodeIds);

    HungryHipposFileSystem hhfs = HungryHipposFileSystem.getInstance();
    hhfs.validateFileDataReady(hungryHippoFilePath);
    FileSystemUtils.createDirectory(outputDirName);
    int dimensionOperand = FileSystemUtils.getDimensionOperand(dimension);
    for (String nodeId : nodeIdSet) {
      String nodeIdZKPath = fileNodeZKDFSPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
      String nodeIp = getNodeIp(nodeId);
      if (isSharded) {
        downloadShardedFile(hungryHippoFilePath, outputDirName, tmpDestFileNames, nodeIdSet,
            dimensionOperand, nodeId, nodeIdZKPath, nodeIp);

      } else {

        downloadUnShardedFile(nodeIdZKPath, Integer.parseInt(nodeId), hungryHippoFilePath,
            outputDirName, nodeIp, tmpDestFileNames);

      }
    }

    combineFiles(tmpDestFileNames, hungryHippoFilePath, outputDirName);
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
  private static void downloadShardedFile(String hungryHippoFilePath, String outputDirName,
      List<String> tmpDestFileNames, Set<String> nodeIdSet, int dimensionOperand, String nodeId,
      String nodeIdZKPath, String nodeIp) throws InterruptedException, ClassNotFoundException,
      JAXBException, KeeperException, IOException {
    long dataFileSize = 0;
    List<String> dataFolderList = null;
    try {
      dataFolderList = curator.getChildren(nodeIdZKPath);

      for (String dataFolder : dataFolderList) {
        String dataFolderZKPath = nodeIdZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + dataFolder;
        int dataFolderIntVal = Integer.parseInt(dataFolder);
        if ((dataFolderIntVal & dimensionOperand) == dimensionOperand) {
          for (String dataFile : nodeIdSet) {
            String dataFileZKPath =
                dataFolderZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + dataFile;

            dataFileSize += (Long) curator.readObject(dataFileZKPath);
          }
        }
      }

      String tmpDestFileName = outputDirName + FileSystemConstants.ZK_PATH_SEPARATOR + nodeIp
          + hungryHippoFilePath.replaceAll(FileSystemConstants.ZK_PATH_SEPARATOR, "-");
      retrieveDataBlocks(nodeIp, Integer.parseInt(nodeId), hungryHippoFilePath, tmpDestFileName,
          dataFileSize, dimensionOperand);
      tmpDestFileNames.add(tmpDestFileName);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

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
   * @throws HungryHippoException
   */

  public static void downloadUnShardedFile(String nodeIdZKPath, int nodeId,
      String hungryHippoFilePath, String outputDirName, String nodeIp,
      List<String> tmpDestFileNames) throws InterruptedException, ClassNotFoundException,
      IOException, KeeperException, JAXBException, HungryHippoException {
    long dataSize = (Long) curator.readObject(nodeIdZKPath);
    String tmpDestFileName = outputDirName + FileSystemConstants.ZK_PATH_SEPARATOR + nodeIp

        + FileSystemConstants.DOWNLOAD_FILE_PREFIX
        + hungryHippoFilePath.replaceAll(FileSystemConstants.ZK_PATH_SEPARATOR, "-");
    retrieveDataBlocks(nodeIp, nodeId, hungryHippoFilePath, tmpDestFileName, dataSize, -1);

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
      String outputFile, long dataSize, int dimensionOperand) throws InterruptedException,
      ClassNotFoundException, JAXBException, KeeperException, IOException {

    int port = FileSystemContext.getServerPort();
    int noOfAttempts = FileSystemContext.getMaxQueryAttempts();
    int fileStreamBufferSize = FileSystemContext.getFileStreamBufferSize();
    long retryTimeInterval = FileSystemContext.getQueryRetryInterval();

    retrieveDataBlocks(nodeIp, nodeId, hungryHippoFilePath, outputFile, fileStreamBufferSize,
        dataSize, dimensionOperand, port, retryTimeInterval, noOfAttempts);
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
      String outputFile, int fileStreamBufferSize, long dataSize, int dimensionOperand, int port,
      long retryTimeInterval, int maxQueryAttempts) {

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
   * @throws JAXBException
   * @throws FileNotFoundException
   */
  public static void retrieveDataBlocks_test(String nodeIp, String hungryHippoFilePath,
      int fileStreamBufferSize, int dataSize, int port, long retryTimeInterval,
      int maxQueryAttempts, String shardingClientConfigLoc)
      throws FileNotFoundException, JAXBException {

    int i = 0;
    ShardingClientConfig shardedConfig =
        JaxbUtil.unmarshalFromFile(shardingClientConfigLoc, ShardingClientConfig.class);
    List<Column> columns = shardedConfig.getInput().getDataDescription().getColumn();
    String[] dataTypeDescription = new String[columns.size()];
    FieldTypeArrayDataDescription dataDescription = null;
    for (int index = 0; index < columns.size(); index++) {
      String element = columns.get(index).getDataType() + "-" + columns.get(index).getSize();
      dataTypeDescription[index] = element;
    }
    dataDescription = FieldTypeArrayDataDescription.createDataDescription(dataTypeDescription,
        shardedConfig.getMaximumSizeOfSingleBlockData());
    DynamicMarshal dm = new DynamicMarshal(dataDescription);

    for (i = 0; i < maxQueryAttempts; i++) {
      try (Socket client = new Socket(nodeIp, port);
          DataOutputStream dos = new DataOutputStream(client.getOutputStream());
          DataInputStream dis = new DataInputStream(client.getInputStream())) {
        String processStatus = dis.readUTF();
        if (FileSystemConstants.DATA_SERVER_AVAILABLE.equals(processStatus)) {
          LOGGER.info("{} Recieving data ", processStatus);
          dos.writeUTF(hungryHippoFilePath);
          dos.writeLong(0);
          int len;
          byte[] inputBuffer = new byte[(int) dataSize];
          while (dataSize > 0) {
            len = dis.read(inputBuffer);
            dataSize -= len;

            int size = dataDescription.getSize();

            int srcPos = 0;
            while (len > 0) {
              int destPos = 0;
              byte[] dest = new byte[size];
              System.arraycopy(inputBuffer, srcPos, dest, destPos, size);
              for (int c = 0; c < dataDescription.getNumberOfDataFields(); c++) {
                Object readableData = dm.readValue(c, ByteBuffer.wrap(dest));

                if (c != 0 && c != dataDescription.getNumberOfDataFields()) {
                  System.out.print(",");
                }
                System.out.print(readableData);
              }
              srcPos = srcPos + size;
              len -= dataDescription.getSize();
              System.out.println();
            }

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
  public static void retrieveDataBlocks_output(String nodeIp, String hungryHippoFilePath,
      int fileStreamBufferSize, int port, long retryTimeInterval, int maxQueryAttempts) {


    int i = 0;
    for (i = 0; i < maxQueryAttempts; i++) {
      try (Socket client = new Socket(nodeIp, port);
          DataOutputStream dos = new DataOutputStream(client.getOutputStream());
          DataInputStream dis = new DataInputStream(client.getInputStream());) {
        String processStatus = dis.readUTF();
        if (FileSystemConstants.DATA_SERVER_AVAILABLE.equals(processStatus)) {
          dos.writeUTF(hungryHippoFilePath);
          dos.writeLong(0);
          int len;
          byte[] inputBuffer = new byte[fileStreamBufferSize];
          long fileSize = fileStreamBufferSize;
          while (fileSize > 0) {
            len = dis.read(inputBuffer);
            if (len < 0) {
              break;
            }

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
