package com.talentica.hungryhippos.filesystem.client;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryhippos.config.coordination.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.Node;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.filesystem.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;

/**
 * This class is for retrieving the sharded files from the HungryHippos
 * Distributed File System Created by rajkishoreh on 29/6/16.
 */
public class DataRetrieverClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataRetrieverClient.class);

    /**
     * This method is for reading the metadata from the zookeeper node and
     * retrieving the data on a default dimension 1
     * @param hungryHippoFilePath
     * @param outputDirName
     * @throws Exception
     */
    public static void getHungryHippoData(String hungryHippoFilePath, String outputDirName) throws Exception {
        getHungryHippoData(hungryHippoFilePath, outputDirName, 1);
    }

    /**
     * This method is for reading the metadata from the zookeeper node and
     * retrieving the data on a particular dimension
     *
     * @param hungryHippoFilePath
     * @param outputDirName
     * @param dimension
     * @throws Exception
     */
    public static void getHungryHippoData(String hungryHippoFilePath, String outputDirName, int dimension) throws Exception {
        String fileName = (new File(hungryHippoFilePath)).getName();
        FileSystemUtils.createDirectory(outputDirName);
        NodesManager nodesManager = NodesManagerContext.getNodesManagerInstance();
        List<String> tmpDestFileNames = new ArrayList<>();
        String fsRootNode = NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting().getFilesystemPath();
        String fileNodeZKDFSPath =fsRootNode+hungryHippoFilePath+File.separator+ FileSystemConstants.DFS_NODE ;
        List<String> nodeIds = nodesManager.getChildren(fileNodeZKDFSPath);
        boolean isSharded = nodesManager.checkNodeExists(fsRootNode+hungryHippoFilePath+File.separator+FileSystemConstants.SHARDED);
        for (String nodeId : nodeIds) {
            String nodeIdZKPath = fileNodeZKDFSPath + File.separator +  nodeId;
            String nodeIp = getNodeIp(nodeId);
            List<String> dataBlockNodes = nodesManager.getChildren(nodeIdZKPath);
            if (isSharded) {
                downloadShardedFile(dataBlockNodes,dimension,nodeIdZKPath,nodesManager,hungryHippoFilePath,outputDirName,nodeIp,tmpDestFileNames);
            } else {
                downloadUnShardedFile(nodeIdZKPath,nodesManager,hungryHippoFilePath,outputDirName,nodeIp,tmpDestFileNames);
            }
        }
        if (!tmpDestFileNames.isEmpty()) {
            FileSystemUtils.combineFiles(tmpDestFileNames, outputDirName + File.separator + fileName);
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
    public static void retrieveDataBlocks(String nodeIp, String hungryHippoFilePath, String outputFile, long dataSize)
            throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {

        int port = FileSystemContext.getServerPort();
        int noOfAttempts = FileSystemContext.getMaxQueryAttempts();
        int fileStreamBufferSize = FileSystemContext.getFileStreamBufferSize();
        long retryTimeInterval = FileSystemContext.getQueryRetryInterval();
        retrieveDataBlocks(nodeIp, hungryHippoFilePath, outputFile, fileStreamBufferSize, dataSize, port,
                retryTimeInterval, noOfAttempts);
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
    public static void retrieveDataBlocks(String nodeIp, String hungryHippoFilePath, String outputFile,
                                          int fileStreamBufferSize, long dataSize, int port, long retryTimeInterval, int maxQueryAttempts) {

        if(dataSize==(new File(outputFile)).length()){
            return;
        }else if(dataSize<(new File(outputFile)).length()){
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
                    long fileSize = dataSize-raf.length();
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

    /**
     * Returns IP of a node using nodeId
     * @param nodeId
     * @return
     */
    public static String getNodeIp(String nodeId){
        ClusterConfig clusterConfig = CoordinationApplicationContext.getCoordinationConfig().getClusterConfig();
        String nodeIp=null;
        for(Node node: clusterConfig.getNode()){
            if(node.getIdentifier()==Integer.parseInt(nodeId)){
                nodeIp = node.getIp();
                break;
            }
        }
        if(nodeIp==null){
            throw new RuntimeException("Cluster configuration not available for nodeId :"+nodeId);
        }
        return nodeIp;
    }

    /**
     * Downloads a sharded files from a node
     *
     * @param dataBlockNodes
     * @param dimension
     * @param nodeIdZKPath
     * @param nodesManager
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
    public static void downloadShardedFile(List<String> dataBlockNodes, int dimension, String nodeIdZKPath, NodesManager nodesManager
            , String hungryHippoFilePath, String outputDirName, String nodeIp, List<String> tmpDestFileNames) throws InterruptedException, ClassNotFoundException, IOException, KeeperException, JAXBException {
        int seq = 0;
        for (String dataBlockNode : dataBlockNodes) {
            int dataBlockIntVal = Integer.parseInt(dataBlockNode);
            int dimensionOperand = FileSystemUtils.getDimensionOperand(dimension);
            if ((dataBlockIntVal & dimensionOperand) == dimensionOperand) {
                String dataBlockZKPath = nodeIdZKPath + File.separator + dataBlockNode;
                String dataBlockSizeStr = (String) nodesManager.getObjectFromZKNode(dataBlockZKPath);
                long dataSize = Long.parseLong(dataBlockSizeStr);
                String filePath = hungryHippoFilePath + File.separator + FileSystemContext.getDataFilePrefix() + dataBlockIntVal;
                String tmpDestFileName = outputDirName + File.separator + nodeIp + FileSystemConstants.DOWNLOAD_FILE_PREFIX +
                        hungryHippoFilePath.replaceAll(File.separator,"-") + seq;
                retrieveDataBlocks(nodeIp, filePath, tmpDestFileName, dataSize);
                tmpDestFileNames.add(tmpDestFileName);
                seq++;
            }
        }
    }

    /**
     * Downloads an Unsharded file from a node
     *
     * @param nodeIdZKPath
     * @param nodesManager
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
    public static void downloadUnShardedFile(String nodeIdZKPath, NodesManager nodesManager
            , String hungryHippoFilePath, String outputDirName, String nodeIp, List<String> tmpDestFileNames) throws InterruptedException, ClassNotFoundException, IOException, KeeperException, JAXBException {
        String dataBlockSizeStr = (String) nodesManager.getObjectFromZKNode(nodeIdZKPath);
        long dataSize = Long.parseLong(dataBlockSizeStr);
        String tmpDestFileName = outputDirName + File.separator + nodeIp + FileSystemConstants.DOWNLOAD_FILE_PREFIX +hungryHippoFilePath.replaceAll(File.separator,"-");
        retrieveDataBlocks(nodeIp, hungryHippoFilePath, tmpDestFileName, dataSize);
        tmpDestFileNames.add(tmpDestFileName);
    }
}
