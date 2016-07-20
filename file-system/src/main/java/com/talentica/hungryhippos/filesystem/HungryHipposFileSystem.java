package com.talentica.hungryhippos.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
/**
 * HungryHipposFileSystem FileSystem. This class has methods for creating files as znodes in the
 * Zookeeper.
 * 
 * @author sudarshans
 *
 */
public class HungryHipposFileSystem {

  private static Logger logger = LoggerFactory.getLogger("ZookeeperFileSystem");
  private static NodesManager nodeManager = null;
	private static final String ROOT_NODE = null;
	// CoordinationApplicationContext.getZkProperty().getValueByKey(FileSystemConstants.ROOT_NODE);
  private static volatile HungryHipposFileSystem hfs = null;

  // for singleton
  private HungryHipposFileSystem() {
    if (hfs != null) {
      throw new IllegalStateException("Instance Already created");
    }

    try {
      nodeManager = NodesManagerContext.getNodesManagerInstance();
    } catch (FileNotFoundException | JAXBException e) {
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
    if (!(name.contains(ROOT_NODE))) {
      if (name.startsWith("/")) {
        name = ROOT_NODE + name;
      } else {
        name = ROOT_NODE + "/" + name;
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
  public String createZnode(String name)  {
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
  public String createZnode(String name, Object data)  {
    name = checkNameContainsFileSystemRoot(name);
    CountDownLatch signal = new CountDownLatch(1);
    return createZnode(name, signal, data);
  }

  /**
   * Create persistent znode in zookeeper
   * 
   * @param name
   * @param signal
   * @return
   */
  public String createZnode(String name, CountDownLatch signal)
       {
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
  public String createZnode(String name, CountDownLatch signal, Object data)
       {
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
  public boolean checkZnodeExists(String name)  {
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
			ZkUtils.getNodePathByName(ROOT_NODE, name, pathWithSameZnodeName);
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
    if (name.equals(ROOT_NODE)) {
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
    if (name.equals(ROOT_NODE)) {
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
   * @param nodeIp
   * @param dataFileZKNode
   * @param datafileSize
   * @throws Exception
   */
  public void updateFSBlockMetaData(String fileZKNode, String nodeIp, String dataFileZKNode,
      long datafileSize) throws Exception {

    String fileNodeZKPath = ROOT_NODE + File.separator + fileZKNode;
    String nodeIpZKPath = fileNodeZKPath + File.separator + nodeIp;
    String dataFileNodeZKPath = nodeIpZKPath + File.separator + dataFileZKNode;
    long prevDataFileSize = 0;
    if (checkZnodeExists(dataFileNodeZKPath)) {
      String prevDataFileData = (String) getObjectData(dataFileNodeZKPath);
      prevDataFileSize = Long.parseLong(prevDataFileData);
      setData(dataFileNodeZKPath, datafileSize + "");
    } else {
      if (!checkZnodeExists(nodeIpZKPath)) {
        createZnode(nodeIpZKPath, new CountDownLatch(1), "");
      }
      createZnode(dataFileNodeZKPath, new CountDownLatch(1), datafileSize + "");
    }
    String fileZKNodeValues = (String) getObjectData(fileNodeZKPath);
    long currentSize = Long.parseLong(fileZKNodeValues) + datafileSize - prevDataFileSize;
    setData(fileNodeZKPath, (currentSize + ""));
  }

}
