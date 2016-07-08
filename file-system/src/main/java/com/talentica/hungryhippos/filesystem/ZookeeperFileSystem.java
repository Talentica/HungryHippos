package com.talentica.hungryhippos.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.coordination.Node;

/**
 * Zookeeper FileSystem. This class has methods for creating files as znodes in
 * the Zookeeper.
 * 
 * @author sudarshans
 *
 */
public class ZookeeperFileSystem {

	private static Logger logger = LoggerFactory.getLogger("ZookeeperFileSystem");

	private FileSystem fs = null;
	private NodesManager nodeManager = null;
	private final String FILE_SYSTEM_ROOT = "/root/file-system/";

	public ZookeeperFileSystem() throws FileNotFoundException, JAXBException {
		nodeManager = NodesManagerContext.getNodesManagerInstance();
	}

	/**
	 * Method used for creating fileName as Znodes.
	 * 
	 * @param fileName
	 */
	public void createFilesAsZnode(String fileName) {

		fs = FileSystems.getDefault();
		Path path = fs.getPath(fileName);
		long size = 0;
		try {
			size = Files.size(path);
		} catch (IOException ioe) {
			logger.error(ioe.getMessage());
		}

		String[] dirStructure = fileName.split(String.valueOf((java.io.File.separatorChar)));
		fileName = fileName.replace(String.valueOf((java.io.File.separatorChar)), "@");
		int length = dirStructure.length;
		String[] nameAndType = dirStructure[length - 1].split("\\.");
		FileMetaData fileMetaData = new FileMetaData(nameAndType[0], nameAndType[1], size);
		final CountDownLatch signal = new CountDownLatch(1);
		try {
			nodeManager.createPersistentNode(FILE_SYSTEM_ROOT + fileName, signal, fileMetaData);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}

	}

	/**
	 * Method used for creating fileName as Znodes.
	 * 
	 * @param fileName
	 */
	public void createNodeDetailsAsZnode(Node node) {

		final CountDownLatch signal = new CountDownLatch(1);
		try {
			nodeManager.createPersistentNode(node.getIp(), signal, null);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}

	}

	/**
	 * To retrieve the data of Znode.
	 * 
	 * @param fileName
	 * @return
	 */

	public FileMetaData getDataInsideZnode(String fileName) {
		fileName = fileName.replace(String.valueOf((java.io.File.separatorChar)), "@");
		FileMetaData fileMetaData = null;
		try {
			Object obj = nodeManager.getObjectFromZKNode(FILE_SYSTEM_ROOT + fileName);
			if (obj instanceof FileMetaData) {
				fileMetaData = (FileMetaData) obj;
			} else {
				logger.error("The serialized message inside the znode is not from FileMetaData class");
			}
		} catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
			logger.error(e.getMessage());
		}

		return fileMetaData;
	}

	/**
	 * To retrieve the data of Znode.
	 * 
	 * @param fileName
	 * @return
	 */

	public FileMetaData getChildren(String fileName) {
		fileName = fileName.replace(String.valueOf((java.io.File.separatorChar)), "@");
		FileMetaData fileMetaData = null;
		try {
			List<String> childNodes = nodeManager.getChildren(FILE_SYSTEM_ROOT + fileName);

		} catch (KeeperException | InterruptedException e) {
			logger.error(e.getMessage());
		}

		return fileMetaData;
	}

	/**
	 * This method updates the HungryHippos filesystem with the metadata of the file
	 *
	 * @param fileZKNode
	 * @param nodeIp
	 * @param dataFileZKNode
	 * @param datafileSize
	 * @throws Exception
	 */
	public void updateFSBlockMetaData(String fileZKNode, String nodeIp, String dataFileZKNode, long datafileSize) throws Exception {
		NodesManager nodesManager = NodesManagerContext.getNodesManagerInstance();
		String fileNodeZKPath = CoordinationApplicationContext.getZkProperty()
				.getValueByKey(FileSystemConstants.ROOT_NODE) +
				File.separator + fileZKNode;
		String nodeIpZKPath = fileNodeZKPath + File.separator + nodeIp;
		String dataFileNodeZKPath = nodeIpZKPath + File.separator + dataFileZKNode;
		long prevDataFileSize = 0;
		if (nodesManager.checkNodeExists(dataFileNodeZKPath)) {
			String prevDataFileData = (String) nodesManager.getObjectFromZKNode(dataFileNodeZKPath);
			prevDataFileSize = Long.parseLong(prevDataFileData);
			nodesManager.setObjectToZKNode(dataFileNodeZKPath, datafileSize + "");
		} else {
			if (!nodesManager.checkNodeExists(nodeIpZKPath)) {
				nodesManager.createPersistentNode(nodeIpZKPath, new CountDownLatch(1), "");
			}
			nodesManager.createPersistentNode(dataFileNodeZKPath, new CountDownLatch(1), datafileSize + "");
		}
		String fileZKNodeValues = (String) nodesManager.getObjectFromZKNode(fileNodeZKPath);
		long currentSize = Long.parseLong(fileZKNodeValues) + datafileSize - prevDataFileSize;
		nodesManager.setObjectToZKNode(fileNodeZKPath, (currentSize + ""));
	}

}
