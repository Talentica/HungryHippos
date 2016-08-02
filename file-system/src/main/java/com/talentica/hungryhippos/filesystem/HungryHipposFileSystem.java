package com.talentica.hungryhippos.filesystem;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZookeeperConfiguration;
import com.talentica.hungryHippos.utility.PathEnum;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

/**
 * 
 * HungryHipposFileSystem FileSystem. This class has methods for creating files
 * as znodes in the Zookeeper.
 * 
 * ======= HungryHipposFileSystem FileSystem. This class has methods for
 * creating files as znodes in the Zookeeper.
 *
 * 
 * @author sudarshans
 *
 */
public class HungryHipposFileSystem {

	private static Logger logger = LoggerFactory.getLogger("HungryHipposFileSystem");
	private static NodesManager nodeManager = null;
	private final String HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER;
	private ZookeeperConfiguration zkConfiguration;
	// CoordinationApplicationContext.getZkProperty().getValueByKey(FileSystemConstants.ROOT_NODE);
	private static volatile HungryHipposFileSystem hfs = null;

  // for singleton
  private HungryHipposFileSystem() {
    if (hfs != null) {
      throw new IllegalStateException("Instance Already created");
    }

		try {
			nodeManager = NodesManagerContext.getNodesManagerInstance();
			CoordinationConfig coordinationConfig =
		          CoordinationApplicationContext.getZkCoordinationConfigCache();
			nodeManager.initializeZookeeperDefaultConfig(coordinationConfig.getZookeeperDefaultConfig());
			HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER = coordinationConfig.getZookeeperDefaultConfig().getFilesystemPath();
		} catch (JAXBException | IOException e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
	}

  public static HungryHipposFileSystem getInstance() {
    if (hfs == null) {
      synchronized (HungryHipposFileSystem.class) {
        if (hfs == null) {
          hfs = new HungryHipposFileSystem();
        }
      }
    }
    return hfs;
  }

	private String checkNameContainsFileSystemRoot(String name) {
		if (!(name.contains(HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER))) {
			if (name.startsWith("/")) {
				name = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + name;
			} else {
				name = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + "/" + name;
			}
		}
		return name;
	}

  /**
   * Create persistent znode in zookeeper
   *
   * @param name
   * @return
   */
  public String createZnode(String name) {
    name = checkNameContainsFileSystemRoot(name);
    CountDownLatch signal = new CountDownLatch(1);
    return createZnode(name, signal);
  }

  /**
   * Create persistent znode in zookeeper and store data on it
   *
   * @param name
   * @param data
   * @return
   */
  public String createZnode(String name, Object data) {
    name = checkNameContainsFileSystemRoot(name);
    CountDownLatch signal = new CountDownLatch(1);
    createZnode(name, signal, data);
    try {
      signal.await();
    } catch (InterruptedException e) {
      logger.error(e.toString());
    }
    return name;
  }

  /**
   * Create persistent znode in zookeeper
   *
   * @param name
   * @param signal
   * @return
   */
  public String createZnode(String name, CountDownLatch signal) {
    name = checkNameContainsFileSystemRoot(name);
    return createZnode(name, signal, "");
  }

  /**
   * Create persistent znode in zookeeper.
   *
   * @param name
   * @param signal
   * @param data
   * @return
   */
  public String createZnode(String name, CountDownLatch signal, Object data) {
    name = checkNameContainsFileSystemRoot(name);
    try {
      nodeManager.createPersistentNode(name, signal, data);
    } catch (IOException e) {
      logger.error(e.getMessage());

    }
    return name;
  }

  /**
   * To check whether a znode already exits on specified directory structure.
   *
   * @param name
   * @return
   */
  public boolean checkZnodeExists(String name) {
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
  public void setData(String name, Object data) {
    name = checkNameContainsFileSystemRoot(name);
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
  public String getData(String name) {
    name = checkNameContainsFileSystemRoot(name);
    String nodeData = null;
    try {
      nodeData = nodeManager.getStringFromZKNode(name);
    } catch (KeeperException | InterruptedException | ClassNotFoundException | IOException e) {
      logger.error(e.getMessage());

    }
    return nodeData;
  }

  /**
   * get value inside the znode in string format.
   *
   * @param name
   * @return
   */
  public Object getObjectData(String name) {
    name = checkNameContainsFileSystemRoot(name);
    Object nodeData = null;

    try {
      nodeData = nodeManager.getObjectFromZKNode(name);
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
      logger.error(e.getMessage());

    }

    return nodeData;
  }

  /**
   * Find the path of a particular znode
   *
   * @param name
   * @return
   */
  public List<String> findZnodePath(String name) {
    List<String> pathWithSameZnodeName = null;
    try {
      ZkUtils.getNodePathByName(HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER, name, pathWithSameZnodeName);
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
  public void deleteNode(String name) {
    if (name.equals(HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER)) {
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
  public void deleteNodeRecursive(String name) {
    if (name.equals(HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER)) {
      logger.info("Cannot delete the root folder.");
      return;
    }
    name = checkNameContainsFileSystemRoot(name);
    CountDownLatch signal = new CountDownLatch(1);
    try {
      ZkUtils.deleteRecursive(name, signal);
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
  public List<String> getChildZnodes(String name) {
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
   * This method updates the HungryHippos filesystem with the metadata of the file
   *
   * @param fileZKNode
   * @param nodeId
   * @param dataFileZKNode
   * @param datafileSize
   * @throws Exception
   */
  public void updateFSBlockMetaData(String fileZKNode, String nodeId, String dataFileZKNode,
      long datafileSize) throws Exception {
    String fileNodeZKPath = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + fileZKNode;
    String fileNodeZKDFSPath =
        fileNodeZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE;
    String nodeIdZKPath = fileNodeZKDFSPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
    String dataFileNodeZKPath =
        nodeIdZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + dataFileZKNode;
    if (!checkZnodeExists(dataFileNodeZKPath)) {
      if (!checkZnodeExists(nodeIdZKPath)) {
        if (!checkZnodeExists(fileNodeZKDFSPath)) {
          createZnode(fileNodeZKDFSPath, "");
        }
        createZnode(nodeIdZKPath, "");
      }
      createZnode(dataFileNodeZKPath, datafileSize + "");
    } else {
      setData(dataFileNodeZKPath, datafileSize + "");
    }
  }

  /**
   * This method updates the HungryHippos filesystem with the metadata of the file
   *
   * @param fileZKNode
   * @param nodeId
   * @param datafileSize
   * @throws Exception
   */
  public void updateFSBlockMetaData(String fileZKNode, String nodeId, long datafileSize)
      throws Exception {
    String fileNodeZKPath = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + fileZKNode;
    String fileNodeZKDFSPath =
        fileNodeZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE;
    String nodeIdZKPath = fileNodeZKDFSPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;

    if (!checkZnodeExists(nodeIdZKPath)) {
      if (!checkZnodeExists(fileNodeZKDFSPath)) {
        createZnode(fileNodeZKDFSPath, "");
      }
      createZnode(nodeIdZKPath, datafileSize + "");
    } else {
      setData(nodeIdZKPath, datafileSize + "");
    }

  }

}
