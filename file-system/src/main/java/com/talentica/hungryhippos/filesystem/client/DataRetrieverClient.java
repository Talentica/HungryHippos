package com.talentica.hungryhippos.filesystem.client;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;
import com.talentica.hungryhippos.filesystem.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.net.Socket;
import java.util.List;

/**
 * This class is for retrieving the sharded files from the HungryHippos
 * Distributed File System Created by rajkishoreh on 29/6/16.
 */
public class DataRetrieverClient {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataRetrieverClient.class);

	/**
	 * This method is for reading the metadata from the zookeeper node and
	 * retrieving the data on a particular dimension
	 *
	 * @param fileZKNode
	 * @param outputDirName
	 * @param dimension
	 * @throws Exception
	 */
	public static void getHungryHippoData(String fileZKNode, String outputDirName, int dimension) throws Exception {
		FileSystemUtils.createDirectory(outputDirName);
		int dimensionOperand = FileSystemUtils.getDimensionOperand(dimension);
		NodesManager nodesManager = NodesManagerContext.getNodesManagerInstance();
		String fileNodeZKPath = CoordinationApplicationContext.getZkProperty()
				.getValueByKey(FileSystemConstants.ROOT_NODE) + File.separator + fileZKNode;
		List<String> nodeIps = nodesManager.getChildren(fileNodeZKPath);
		int count = 0;
		for (String nodeIp : nodeIps) {
			long dataSize = 0;
			String nodeIpZKPath = fileNodeZKPath + File.separator + nodeIp;
			List<String> dataBlockNodes = nodesManager.getChildren(nodeIpZKPath);
			StringBuilder dataBlocks = new StringBuilder();
			boolean isFirstDataNode = true;
			for (String dataBlockNode : dataBlockNodes) {
				int dataBlockIntVal = Integer.parseInt(dataBlockNode);
				if ((dataBlockIntVal & dimensionOperand) == dimensionOperand) {
					String dataBlockZKPath = nodeIpZKPath + File.separator + dataBlockNode;
					String dataBlockSizeStr = (String) nodesManager.getObjectFromZKNode(dataBlockZKPath);
					dataSize += Long.parseLong(dataBlockSizeStr);
					if (isFirstDataNode) {
						dataBlocks.append(dataBlockIntVal);
						isFirstDataNode = false;
					} else {
						dataBlocks.append(FileSystemConstants.FILE_PATHS_DELIMITER).append(dataBlockIntVal);
					}
				}
			}
			LOGGER.info(dataBlocks.toString());
			retrieveDataBlocks(nodeIp, fileZKNode, dataBlocks.toString(),
					outputDirName + File.separator + FileSystemConstants.DOWNLOAD_FILE_PREFIX + count++, dataSize);
		}
	}

	/**
	 * This method forwards the request with configuration for retrieving data
	 *
	 * @param nodeIp
	 * @param fileZKNode
	 * @param dataBlocks
	 * @param outputFile
	 * @param dataSize
	 */
	public static void retrieveDataBlocks(String nodeIp, String fileZKNode, String dataBlocks, String outputFile,
			long dataSize)
			throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {

		int port = FileSystemContext.getServerPort();
		int noOfAttempts = FileSystemContext.getMaxQueryAttempts();
		int fileStreamBufferSize = FileSystemContext.getFileStreamBufferSize();
		long retryTimeInterval = FileSystemContext.getQueryRetryInterval();
		retrieveDataBlocks(nodeIp, fileZKNode, dataBlocks, outputFile, fileStreamBufferSize, dataSize, port,
				retryTimeInterval, noOfAttempts);
	}

	/**
	 * This method is for requesting the DataRequestHandlerServer for the files
	 *
	 * @param nodeIp
	 * @param fileZKNode
	 * @param dataBlocks
	 * @param outputFile
	 * @param dataSize
	 * @param port
	 * @param retryTimeInterval
	 * @param maxQueryAttempts
	 */
	public static void retrieveDataBlocks(String nodeIp, String fileZKNode, String dataBlocks, String outputFile,
			int fileStreamBufferSize, long dataSize, int port, long retryTimeInterval, int maxQueryAttempts) {
		int i = 0;
		for (i = 0; i < maxQueryAttempts; i++) {
			try (Socket client = new Socket(nodeIp, port);
					DataOutputStream dos = new DataOutputStream(client.getOutputStream());
					DataInputStream dis = new DataInputStream(client.getInputStream());
					FileOutputStream fos = new FileOutputStream(outputFile);
					BufferedOutputStream bos = new BufferedOutputStream(fos);) {
				String processStatus = dis.readUTF();
				if (FileSystemConstants.DATA_SERVER_AVAILABLE.equals(processStatus)) {
					LOGGER.info("{} Recieving data into {}", processStatus, outputFile);
					dos.writeUTF(fileZKNode);
					dos.writeUTF(dataBlocks);
					int len;
					byte[] inputBuffer = new byte[fileStreamBufferSize];
					long fileSize = dataSize;
					while (fileSize > 0 && (len = dis.read(inputBuffer)) > -1) {
						bos.write(inputBuffer, 0, len);
						fileSize = fileSize - len;
					}
					bos.flush();
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
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
		}
		if (i == maxQueryAttempts) {
			throw new RuntimeException("Data Retrieval Failed");
		}
	}
}
