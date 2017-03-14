package com.talentica.hungryhippos.filesystem;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * 
 * {@code HungryHipposFileSystem} creating files as znodes in the Zookeeper.
 * 
 * 
 * @author sudarshans
 *
 */
public class HungryHipposFileSystem {

  private static Logger logger = LoggerFactory.getLogger("HungryHipposFileSystem");
  private static HungryHippoCurator curator = null;
  private final String HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER;
  private static volatile HungryHipposFileSystem hhfs = null;
  private static String HUNGRYHIPPOS_FS_NODE = null;



  // for singleton
  private HungryHipposFileSystem() throws FileNotFoundException, JAXBException {

    if (hhfs != null) {
      throw new IllegalStateException("Instance Already created");
    }
    curator = HungryHippoCurator.getInstance();
    HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER =
        CoordinationConfigUtil.getFileSystemPath();
    HUNGRYHIPPOS_FS_NODE = FileSystemContext.getRootDirectory();

  }

  /**
   * retrieves HungryHipposFileSystem node that was set.
   * 
   * @return String representing the HungryHippoFileSystem location.
   */
  public String getHHFSNodeRoot() {
    return HUNGRYHIPPOS_FS_NODE;
  }

  /**
   * used for create HungryHipposFileSystem instance if it was not created previously. Else
   * retrieves the one which was created previously.
   * 
   * @return
   * @throws FileNotFoundException
   * @throws JAXBException
   */
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

  /**
   * checks whether the {@value name } provided contains the FileSystem root in it. if not it
   * appends it.
   * 
   * @param name
   * @return String representing the path that HungryHippoFileSystem will use.
   */
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
    try {
      name = curator.createPersistentNodeIfNotPresent(name);
    } catch (HungryHippoException e) {
      logger.error(e.getMessage());
    }
    return name;
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
    name = curator.createPersistentNodeIfNotPresent(name, data);

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
      exists = curator.checkExists(name);
    } catch (HungryHippoException e) {
      logger.error(e.getMessage());
    }
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
    Stat stat = null;
    try {
      stat = curator.getStat(name);
    } catch (HungryHippoException e) {
      logger.error(e.getMessage());
    }
    return stat;
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
      curator.setZnodeData(name, data);
    } catch (HungryHippoException e) {
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

    try {
      nodeData = curator.getZnodeData(name);

    } catch (HungryHippoException e) {
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
      nodeData = curator.readObject(name);
    } catch (HungryHippoException e) {
      logger.error(e.getMessage());

    }

    return nodeData;
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

    try {
      curator.delete(name);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
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

    try {
      curator.deleteRecursive(name);
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
    List<String> childZnodes = new ArrayList<>();
    name = checkNameContainsFileSystemRoot(name);
    try {
      childZnodes.addAll(curator.getChildren(name));

    } catch (HungryHippoException e) {
      logger.error(e.getMessage());

    }
    return Collections.unmodifiableList(childZnodes);
  }

  /**
   * This method updates the HungryHippos filesystem with the metadata of the file
   * 
   * @param hungryHippoFilePath
   * @param nodeId
   * @param dataFileId
   * @param fileSize
   * @throws Exception
   */
  public void updateFSBlockMetaData(String hungryHippoFilePath, int nodeId, String dataFileId,
                                    long fileSize) throws Exception {
    String hungryHippoFileZKPath = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + hungryHippoFilePath;
    String dfsZKPath = hungryHippoFileZKPath + FileSystemConstants.ZK_PATH_SEPARATOR
        + FileSystemConstants.DFS_NODE;
    String nodeIdZKPath = dfsZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
    String dataFileNodeZKPath = nodeIdZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + dataFileId;
    curator.createPersistentNodeIfNotPresent(dfsZKPath);
    curator.createPersistentNodeIfNotPresent(nodeIdZKPath);
    curator.createPersistentNodeIfNotPresent(dataFileNodeZKPath);
    curator.createPersistentNodeIfNotPresent(dataFileNodeZKPath, fileSize);
  }

  /**
   * This method updates the HungryHippos filesystem with the metadata of the file
   *
   * @param fileZKNode
   * @param nodeId
   * @param datafileSize
   * @throws HungryHippoException
   * @throws Exception
   */

  public void updateFSBlockMetaData(String fileZKNode, int nodeId, long datafileSize)
      throws HungryHippoException {

    String fileNodeZKPath = HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER + fileZKNode;
    logger.info("HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER " + HUNGRYHIPPOS_FS_ROOT_ZOOKEEPER);
    String fileNodeZKDFSPath =
        fileNodeZKPath + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE;
    String nodeIdZKPath = fileNodeZKDFSPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
    synchronized (this) {
      curator.createPersistentNodeIfNotPresent(fileNodeZKDFSPath);
      curator.updatePersistentNode(nodeIdZKPath, datafileSize);
    }
  }

  /**
   * Validates if file Data is ready
   * 
   * @param hungryHippoFilePath
   * @throws HungryHippoException
   */
  public void validateFileDataReady(String hungryHippoFilePath) throws HungryHippoException {
    if (null == hungryHippoFilePath || hungryHippoFilePath.isEmpty()) {
      throw new RuntimeException("Path is null or empty");
    }
    String fsRootNode = CoordinationConfigUtil.getFileSystemPath();
    String hungryHippoFilePathNode = fsRootNode + hungryHippoFilePath;
    boolean nodeExists = curator.checkExists(hungryHippoFilePathNode);
    if (!nodeExists) {
      throw new RuntimeException(hungryHippoFilePath + " does not exists");
    }
    String data = (String) curator.getZnodeData(hungryHippoFilePathNode);
    if (!FileSystemConstants.IS_A_FILE.equals(data)) {
      throw new RuntimeException(hungryHippoFilePath + " is not a file");
    }
    String dataReadyNode = hungryHippoFilePathNode + FileSystemConstants.ZK_PATH_SEPARATOR
        + FileSystemConstants.DATA_READY;
    boolean isDataReady = curator.checkExists(dataReadyNode);
    while (!isDataReady) {
      isDataReady = curator.checkExists(dataReadyNode);
    }
  }

  public long size(String path) {

    long size = 0;
    boolean flag = false;
    List<String> childNodes = hhfs.getChildZnodes(path);
    if (childNodes.contains(FileSystemConstants.SHARDED)) {
      flag = childNodes.contains(FileSystemConstants.DFS_NODE);

      if (!flag) {
        return 0;
      }

      List<String> nodeDetails = hhfs.getChildZnodes(
          path + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE);
      for (String node : nodeDetails) {
        List<String> dataFolders = hhfs.getChildZnodes(path + FileSystemConstants.ZK_PATH_SEPARATOR
            + FileSystemConstants.DFS_NODE + FileSystemConstants.ZK_PATH_SEPARATOR + node);
        for (String dataFolder : dataFolders) {
          List<String> nodeIds = hhfs.getChildZnodes(path + FileSystemConstants.ZK_PATH_SEPARATOR
              + FileSystemConstants.DFS_NODE + FileSystemConstants.ZK_PATH_SEPARATOR + node
              + FileSystemConstants.ZK_PATH_SEPARATOR + dataFolder);
          for (String nodeId : nodeIds) {
            long length = (long) hhfs.getObjectData(path + FileSystemConstants.ZK_PATH_SEPARATOR
                + FileSystemConstants.DFS_NODE + FileSystemConstants.ZK_PATH_SEPARATOR + node
                + FileSystemConstants.ZK_PATH_SEPARATOR + dataFolder
                + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId);
            size += length;
          }

        }

      }
    } else {

      flag = childNodes.contains(FileSystemConstants.DFS_NODE);

      if (!flag) {
        return 0;
      }

      List<String> nodeDetails = hhfs.getChildZnodes(
          path + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE);
      for (String node : nodeDetails) {
        long length = (long) hhfs.getObjectData(path + FileSystemConstants.ZK_PATH_SEPARATOR
            + FileSystemConstants.DFS_NODE + FileSystemConstants.ZK_PATH_SEPARATOR + node);
        size += length;
      }

    }
    return size;
  }
}
