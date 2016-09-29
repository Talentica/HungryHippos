package com.talentica.hungryhippos.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZookeeperConfiguration;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * 
 * HungryHipposFileSystem FileSystem. This class has methods for creating files as znodes in the
 * Zookeeper.
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
  private static volatile HungryHipposFileSystem hhfs = null;
  private static String HUNGRYHIPPOS_FS_NODE = null;



  // for singleton
  private HungryHipposFileSystem() throws FileNotFoundException, JAXBException {

    if (hhfs != null) {
      throw new IllegalStateException("Instance Already created");
    }
    nodeManager = NodesManagerContext.getNodesManagerInstance();
    CoordinationConfig coordinationConfig = CoordinationConfigUtil.getZkCoordinationConfigCache();
    HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER =
        coordinationConfig.getZookeeperDefaultConfig().getFilesystemPath();
    HUNGRYHIPPOS_FS_NODE = FileSystemContext.getRootDirectory();

  }

  public String getHHFSNodeRoot() {
    return HUNGRYHIPPOS_FS_NODE;
  }

  public String getHHFSZROOT() {
    return this.HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER;
  }

  public static HungryHipposFileSystem getInstance() throws FileNotFoundException, JAXBException {
    if (hhfs == null) {
      synchronized (HungryHipposFileSystem.class) {
        if (hhfs == null) {
          hhfs = new HungryHipposFileSystem();
        }
      }
    }
    return hhfs;
  }

  public String checkNameContainsFileSystemRoot(String name) {
    if (!(name.contains(HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER))) {
      if (name.startsWith(FileSystemConstants.ZK_PATH_SEPARATOR)) {
        name = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + name;
      } else {
        name = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + FileSystemConstants.ZK_PATH_SEPARATOR + name;
      }
    }
    if (name.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)) {
      name = name.substring(0, name.length() - 1);
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
      signal.await();

    } catch (IOException e) {
      logger.error(e.getMessage());

    } catch (InterruptedException e) {
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
    exists = nodeManager.checkNodeExists(name);
    return exists;
  }

  /**
   * To check whether a znode already exits on specified directory structure.
   *
   * @param name
   * @return
   */
  public Stat getZnodeStat(String name) {
    name = checkNameContainsFileSystemRoot(name);

    return ZkUtils.getStat(name);
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
  public String getNodeData(String name) {
    name = checkNameContainsFileSystemRoot(name);
    String nodeData = null;

    nodeData = (String) ZkUtils.getNodeData(name);

    return nodeData;
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
      Object data = nodeManager.getObjectFromZKNode(name);
      if (data != null) {
        nodeData = (String) data;
      }
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
    name = checkNameContainsFileSystemRoot(name);
    if (name.equals(HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER)) {
      logger.info("Cannot delete the root folder.");
      return;
    }

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
      if (childZnodes == null) {
        childZnodes = new ArrayList<>();
      }
    } catch (KeeperException | InterruptedException e) {
      logger.error(e.getMessage());

    }
    return childZnodes;
  }

  /**
   * This method updates the HungryHippos filesystem with the metadata of the file
   * 
   * @param hungryHippoFilePath
   * @param nodeId
   * @param dataFileId
   * @param fileName
   * @param fileSize
   * @throws Exception
   */
  public void updateFSBlockMetaData(String hungryHippoFilePath, String nodeId, int dataFileId,
      String fileName, long fileSize) throws Exception {
    String hungryHippoFileZKPath = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + hungryHippoFilePath;
    String dfsZKPath = hungryHippoFileZKPath + FileSystemConstants.ZK_PATH_SEPARATOR
        + FileSystemConstants.DFS_NODE;
    String nodeIdZKPath = dfsZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
    String dataFileNodeZKPath = nodeIdZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + dataFileId;
    String fileNodeZKPath = dataFileNodeZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + fileName;
    ZkUtils.createZKNodeIfNotPresent(dfsZKPath, "");
    ZkUtils.createZKNodeIfNotPresent(nodeIdZKPath, "");
    ZkUtils.createZKNodeIfNotPresent(dataFileNodeZKPath, "");
    ZkUtils.createZKNode(fileNodeZKPath, fileSize + "");
  }

  /**
   * This method updates the HungryHippos filesystem with the metadata of the file
   *
   * @param fileZKNode
   * @param nodeId
   * @param datafileSize
   * @throws Exception
   */
  public void updateFSBlockMetaData(String fileZKNode, String nodeId, long datafileSize) {
    String fileNodeZKPath = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + fileZKNode;
    logger.info("HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER " + HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER);
    String fileNodeZKDFSPath =
        fileNodeZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE;
    String nodeIdZKPath = fileNodeZKDFSPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
    ZkUtils.createZKNodeIfNotPresent(fileNodeZKDFSPath, "");
    ZkUtils.createZKNode(nodeIdZKPath, datafileSize + "");
  }

  /**
   * Validates if file Data is ready
   * 
   * @param hungryHippoFilePath
   */
  public static void validateFileDataReady(String hungryHippoFilePath) {
    if (null == hungryHippoFilePath || hungryHippoFilePath.isEmpty()) {
      throw new RuntimeException("Path is null or empty");
    }
    String fsRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath();
    String hungryHippoFilePathNode = fsRootNode + hungryHippoFilePath;
    boolean nodeExists = ZkUtils.checkIfNodeExists(hungryHippoFilePathNode);
    if (!nodeExists) {
      throw new RuntimeException(hungryHippoFilePath + " does not exists");
    }
    String data = (String) ZkUtils.getNodeData(hungryHippoFilePathNode);
    if (!FileSystemConstants.IS_A_FILE.equals(data)) {
      throw new RuntimeException(hungryHippoFilePath + " is not a file");
    }
    String dataReadyNode = hungryHippoFilePathNode + FileSystemConstants.ZK_PATH_SEPARATOR
        + FileSystemConstants.DATA_READY;
    boolean isDataReady = ZkUtils.checkIfNodeExists(dataReadyNode);
    if (!isDataReady) {
      throw new RuntimeException(hungryHippoFilePath + " file is not ready");
    }
  }

}
