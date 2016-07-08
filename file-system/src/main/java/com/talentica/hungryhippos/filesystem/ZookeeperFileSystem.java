package com.talentica.hungryhippos.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import com.talentica.hungryHippos.coordination.ZKUtils;
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
	private static NodesManager nodeManager = null;
	private static final String FILE_SYSTEM_ROOT = "/root/file-system/";

	private static void getZookeeperFileSystem() {
		try {
			nodeManager = NodesManagerContext.getNodesManagerInstance();
		} catch (FileNotFoundException | JAXBException e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
	}

	private static String checkNameContainsFileSystemRoot(String name) {
		if (!(name.contains(FILE_SYSTEM_ROOT))) {
			name = FILE_SYSTEM_ROOT + name;
		}
		return name;
	}

	static {
		getZookeeperFileSystem();
	}

	/**
	 * Create persistent znode in zookeeper
	 * 
	 * @param name
	 * @return
	 */
	public static void createZnode(String name) {
		name = checkNameContainsFileSystemRoot(name);
		CountDownLatch signal = new CountDownLatch(1);
		createZnode(name, signal);
	}

	/**
	 * Create persistent znode in zookeeper and store data on it
	 * 
	 * @param name
	 * @param data
	 * @return
	 */
	public static void createZnode(String name, Object data) {
		name = checkNameContainsFileSystemRoot(name);
		CountDownLatch signal = new CountDownLatch(1);
		createZnode(name, signal, data);
	}

	/**
	 * Create persistent znode in zookeeper
	 * 
	 * @param name
	 * @param signal
	 * @return
	 */
	public static void createZnode(String name, CountDownLatch signal) {
		name = checkNameContainsFileSystemRoot(name);
		createZnode(name, signal, "");
	}

	/**
	 * Create persistent znode in zookeeper.
	 * 
	 * @param name
	 * @param signal
	 * @param data
	 * @return
	 */
	public static void createZnode(String name, CountDownLatch signal, Object data) {
		name = checkNameContainsFileSystemRoot(name);
		try {
			nodeManager.createPersistentNode(name, signal, data);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * To check whether a znode already exits on specified directory structure.
	 * 
	 * @param name
	 * @return
	 */
	public static boolean checkZnodeExists(String name) {
		name = checkNameContainsFileSystemRoot(name);
		boolean exists = false;
		try {
			exists = nodeManager.checkNodeExists(name);
		} catch (InterruptedException | KeeperException e) {
			logger.error(e.getMessage());
		}
		return exists;
	}

	/**
	 * set znode value to the data.
	 * 
	 * @param name
	 * @param data
	 */
	public static void setData(String name, Object data) {
		try {
			nodeManager.setObjectToZKNode(name, data);
		} catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * get value inside the znode in string format.
	 * 
	 * @param name
	 * @return
	 */
	public static String getData(String name) {
		name = checkNameContainsFileSystemRoot(name);
		byte[] nodeData = null;
		try {
			nodeData = nodeManager.getNodeData(name);
		} catch (KeeperException | InterruptedException e) {
			logger.error(e.getMessage());
		}
		return new String(nodeData, StandardCharsets.UTF_8);
	}

	/**
	 * Find the path of a particular znode
	 * 
	 * @param name
	 * @return
	 */
	public static List<String> findZnodePath(String name) {
		List<String> pathWithSameZnodeName = null;
		try {
			ZKUtils.getNodePathByName(FILE_SYSTEM_ROOT, name, pathWithSameZnodeName);
		} catch (InterruptedException | KeeperException e) {
			logger.error(e.getMessage());
		}
		return pathWithSameZnodeName;
	}

	/**
	 * Delete the specified node. Only deletes when the file is
	 * 
	 * @param name
	 */
	public static void deleteNode(String name) {
		if (name.equals(FILE_SYSTEM_ROOT)) {
			logger.info("Cannot delete the root folder.");
			return;
		}
		name = checkNameContainsFileSystemRoot(name);
		nodeManager.deleteNode(name);
	}

	/**
	 * Delete the specified zode and all the children folders.
	 * 
	 * @param name
	 */
	public static void deleteNodeRecursive(String name) {
		if (name.equals(FILE_SYSTEM_ROOT)) {
			logger.info("Cannot delete the root folder.");
			return;
		}
		name = checkNameContainsFileSystemRoot(name);
		CountDownLatch signal = new CountDownLatch(1);
		try {
			ZKUtils.deleteRecursive(name, signal);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * returns a list of children znodes of the node specified.
	 * 
	 * @param name
	 * @return
	 */
	public static List<String> getChildZnodes(String name) {
		List<String> childZnodes = null;
		name = checkNameContainsFileSystemRoot(name);
		try {
			childZnodes = nodeManager.getChildren(name);
		} catch (KeeperException | InterruptedException e) {
			logger.error(e.getMessage());
		}
		return childZnodes;
	}

	/**
	 * This method updates the HungryHippos filesystem with the metadata of the
	 * file
	 *
	 * @param fileZKNode
	 * @param nodeIp
	 * @param dataFileZKNode
	 * @param datafileSize
	 * @throws Exception
	 */
	public static void updateFSBlockMetaData(String fileZKNode, String nodeIp, String dataFileZKNode, long datafileSize)
			throws Exception {

		String fileNodeZKPath = CoordinationApplicationContext.getZkProperty()
				.getValueByKey(FileSystemConstants.ROOT_NODE) + File.separator + fileZKNode;
		String nodeIpZKPath = fileNodeZKPath + File.separator + nodeIp;
		String dataFileNodeZKPath = nodeIpZKPath + File.separator + dataFileZKNode;
		long prevDataFileSize = 0;
		if (nodeManager.checkNodeExists(dataFileNodeZKPath)) {
			String prevDataFileData = (String) nodeManager.getObjectFromZKNode(dataFileNodeZKPath);
			prevDataFileSize = Long.parseLong(prevDataFileData);
			nodeManager.setObjectToZKNode(dataFileNodeZKPath, datafileSize + "");
		} else {
			if (!nodeManager.checkNodeExists(nodeIpZKPath)) {
				nodeManager.createPersistentNode(nodeIpZKPath, new CountDownLatch(1), "");
			}
			nodeManager.createPersistentNode(dataFileNodeZKPath, new CountDownLatch(1), datafileSize + "");
		}
		String fileZKNodeValues = (String) nodeManager.getObjectFromZKNode(fileNodeZKPath);
		long currentSize = Long.parseLong(fileZKNodeValues) + datafileSize - prevDataFileSize;
		nodeManager.setObjectToZKNode(fileNodeZKPath, (currentSize + ""));
	}

}
