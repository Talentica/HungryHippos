/**
 * 
 */
package com.talentica.hungryHippos.coordination;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.annotations.ZkTransient;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryhippos.config.client.CoordinationServers;

/**
 * 
 * Zookeeper utility to perform various handy operation.
 * 
 * @author PooshanS
 *
 */
public class ZkUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class.getName());
  private static String ZK_ROOT_NODE = "/rootnode";
  public static ZooKeeper zk;
  public static NodesManager nodesManager;

  public static ZKNodeFile getConfigZKNodeFile(String fileName,
      CoordinationServers coordinationServers) throws FileNotFoundException, JAXBException {
    Object obj = null;
    ZKNodeFile zkFile = null;
    nodesManager = NodesManagerContext.getNodesManagerInstance();
    try {
      obj = nodesManager.getConfigFileFromZNode(fileName);
      zkFile = (obj == null) ? null : (ZKNodeFile) obj;
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
      LOGGER.error("Error occurred while getting zk file.", e);
    }
    return zkFile;
  }

  /**
   * To serialize the object
   * 
   * @param obj
   * @return byte[]
   * @throws IOException
   */
  public static byte[] serialize(Object obj) throws IOException {
    return SerializationUtils.serialize((Serializable) obj);
  }

  /**
   * To deserialize the byte[]
   * 
   * @param obj
   * @return Object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static Object deserialize(byte[] obj) throws IOException, ClassNotFoundException {
    return (obj == null) ? null : SerializationUtils.deserialize(obj);
  }

  /**
   * Get current timestamp in format yyyy-MM-dd_HH:mm:ss
   * 
   * @return
   */
  public static String getCurrentTimeStamp() {
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
    Date now = new Date();
    String strDate = sdfDate.format(now);
    return strDate;
  }

  /**
   * To search the particular node of the zookeeper.
   * 
   * @param searchString
   * @param authRole
   * @return Set<LeafBean>
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static Set<LeafBean> searchLeafNode(String searchString, String authRole,
      CountDownLatch signal) throws InterruptedException, KeeperException, IOException,
      ClassNotFoundException {
    LOGGER.info("IN searchTree path {}", searchString);
    /* Export all nodes and then search. */
    if (searchString.contains("/"))
      ZK_ROOT_NODE = searchString;
    Set<LeafBean> searchResult = new TreeSet<>();
    Set<LeafBean> leaves = new TreeSet<>();
    exportTreeInternal(leaves, ZK_ROOT_NODE, authRole);
    for (LeafBean leaf : leaves) {
      if (leaf.getPath().contains(searchString) || leaf.getName().contains(searchString)) {
        searchResult.add(leaf);
      }
    }
    if (signal != null)
      signal.countDown();
    return searchResult;

  }

  private static void exportTreeInternal(Set<LeafBean> entries, String path, String authRole)
      throws InterruptedException, KeeperException, ClassNotFoundException, IOException {

    entries.addAll(listLeaves(path, authRole)); // List leaves

    /* Process folders */
    for (String folder : listFolders(path)) {
      exportTreeInternal(entries, getNodePath(path, folder), authRole);
    }
  }

  public static List<LeafBean> listLeaves(String path, String authRole)
      throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
    List<LeafBean> leaves = new ArrayList<>();

    List<String> children = zk.getChildren(path, false);
    if (children != null) {
      for (String child : children) {
        String childPath = getNodePath(path, child);
        List<String> subChildren = Collections.emptyList();
        subChildren = zk.getChildren(childPath, false);
        boolean isFolder = subChildren != null && !subChildren.isEmpty();
        if (!isFolder) {
          leaves.add(getNodeValue(path, childPath, child, authRole));
        }
      }
    }

    Collections.sort(leaves, new Comparator<LeafBean>() {
      @Override
      public int compare(LeafBean o1, LeafBean o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    return leaves;
  }

  public static List<String> listFolders(String path) throws KeeperException, InterruptedException {
    List<String> folders = new ArrayList<>();
    List<String> children = zk.getChildren(path, false);
    if (children != null) {
      for (String child : children) {
        List<String> subChildren = zk.getChildren(path + PathUtil.SEPARATOR_CHAR + child, false);
        boolean isFolder = subChildren != null && !subChildren.isEmpty();
        if (isFolder) {
          folders.add(child);
        }
      }
    }

    Collections.sort(folders);
    return folders;
  }

  public static String getNodePath(String path, String name) {
    return path + PathUtil.SEPARATOR_CHAR + name;

  }

  public static void getNodePathByName(String path, String name, List<String> nodePathList)
      throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren(path, false);
    for (String child : children) {
      String childPath;
      if (!path.equals("/")) {
        childPath = path + "/" + child;
      } else {
        childPath = path + child;
      }
      if (child.equals(name)) {
        nodePathList.add(childPath);
      }
      getNodePathByName(childPath, name, nodePathList);
    }
    return;
  }



  /**
   * To get the value of particluar node
   * 
   * @param path
   * @param childPath
   * @param child
   * @param authRole
   * @return
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  public static LeafBean getNodeValue(String path, String childPath, String child, String authRole)
      throws ClassNotFoundException, IOException, KeeperException, InterruptedException {
    return getNodeDetail(path, childPath, child, authRole, true);
  }

  public static LeafBean getNodeDetail(String path, String childPath, String child,
      String authRole, boolean getData) throws KeeperException, InterruptedException,
      ClassNotFoundException, IOException {
    try {
      byte[] dataBytes = null;
      Stat stat = zk.exists(childPath, nodesManager);
      if (stat != null && getData) {
        dataBytes = zk.getData(childPath, nodesManager, stat);
      }
      return (new LeafBean(path, child, dataBytes));
    } catch (KeeperException | InterruptedException ex) {
      LOGGER.error(ex.getMessage());
    }
    return null;
  }

  public static Object externalizeNodeValue(byte[] value) throws ClassNotFoundException,
      IOException {
    if (value == null)
      return null;
    return ZkUtils.deserialize(value);
  }

  public static String buildNodePath(int nodeId) {
    return NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting()
        .getHostPath()
        + PathUtil.SEPARATOR_CHAR + ("_node" + nodeId);
  }

  public static String buildNodePath(String jobuuid) {
    return NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting()
        .getHostPath()
        + PathUtil.SEPARATOR_CHAR + (jobuuid);
  }

  /**
   * Delete all the nodes of zookeeper recursively
   * 
   * @param node
   * @throws InterruptedException
   * @throws KeeperException
   * @throws Exception
   */
  public static void deleteRecursive(String node, CountDownLatch signal) throws Exception {
    try {
      Stat stat = zk.exists(node, true);
      if (stat == null) {
        LOGGER.info("No such node {} exists.", node);
        return;
      }
      ZKUtil.deleteRecursive(zk, node);
      LOGGER.info("Nodes are deleted recursively");
    } catch (InterruptedException | KeeperException e) {
      LOGGER.info("\tUnable to delete the node Exception :: " + e.getMessage());
    } finally {
      if (signal != null)
        signal.countDown();
    }
  }

  /**
   * Async call to check whether path exists or not in zookeeper node.
   * 
   * @param signal
   * @return
   */
  public static AsyncCallback.StatCallback checkPathExistsStatusAsync(CountDownLatch signal) {
    AsyncCallback.StatCallback checkPathExistsCallback = new AsyncCallback.StatCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
          case CONNECTIONLOSS:
            LOGGER
                .info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO CHECK STATUS...");
            break;
          case OK:
            LOGGER.info("ZOOKEEPER SERVER IS RUNNING...");
            break;
          case NONODE:
            try {
              ZkUtils.waitForSignal(path, signal);
            } catch (KeeperException | InterruptedException e) {
              LOGGER.error("Error occurred in async callback.", e);
            }
            break;
          case NODEEXISTS:
            LOGGER.info("Node {} exists", path);
            break;
          default:
            LOGGER.info("Unexpected result for path {} exists", path);
        }
      }
    };
    return checkPathExistsCallback;
  }

  /**
   * Check delete node status Async on zookeeper.
   * 
   * @return
   */
  public static AsyncCallback.VoidCallback checkDeleteNodeStatusAsync() {
    AsyncCallback.VoidCallback deleteNodeCallback = new AsyncCallback.VoidCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx) {
        String node = (String) ctx;
        switch (KeeperException.Code.get(rc)) {
          case CONNECTIONLOSS:
            nodesManager.deleteNode(node);
            LOGGER
                .info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
            break;
          case OK:
            LOGGER.info("Node {} is  ({})", node, path);
            break;
          default:
            LOGGER.info("[{}] Unexpected result for deleting {} ({})", new Object[] {
                KeeperException.Code.get(rc), node, path});
        }
      }
    };
    return deleteNodeCallback;
  }

  /**
   * Check zookeeper connection Async on zookeeper
   * 
   * @return
   */
  public static AsyncCallback.StatCallback checkZKConnectionStatusAsync() {
    AsyncCallback.StatCallback checkStatusCallback = new AsyncCallback.StatCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, Stat stat) {
        Server svr = (Server) ctx;
        switch (KeeperException.Code.get(rc)) {
          case CONNECTIONLOSS:
            LOGGER
                .info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO CHECK STATUS...");
            nodesManager.checkZookeeperConnection(svr);
            break;
          case OK:
            LOGGER.info("ZOOKEEPER SERVER IS RUNNING...");
            LOGGER.info("Node {} is  ({})", svr.getName(), svr.getServerAddress().getHostname());
            break;
          default:
            LOGGER.info("[{}] Unexpected result for STATUS {} ({})", new Object[] {
                KeeperException.Code.get(rc), svr.getName(), path});
        }
      }
    };
    return checkStatusCallback;
  }

  /**
   * Check server delete status Async on zookeeper
   * 
   * @return
   */
  public static AsyncCallback.VoidCallback checkServerDeleteStatusAsync() {
    AsyncCallback.VoidCallback deleteCallback = new AsyncCallback.VoidCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx) {
        Server svr = (Server) ctx;
        switch (KeeperException.Code.get(rc)) {
          case CONNECTIONLOSS:
            nodesManager.deleteNode(svr);
            LOGGER
                .info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
            break;
          case OK:
            LOGGER.info("Node {} is  ({})", svr.getName(), svr.getServerAddress().getHostname());
            break;
          default:
            LOGGER.info("[{}] Unexpected result for deleting {} ({})", new Object[] {
                KeeperException.Code.get(rc), svr.getName(), path});
        }
      }
    };
    return deleteCallback;
  }

  /**
   * Check romove status of Alert Async on zookeeper
   * 
   * @return
   */
  public static AsyncCallback.VoidCallback checkAlertRemoveStatusAsync() {
    AsyncCallback.VoidCallback removeAlertCallback = new AsyncCallback.VoidCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx) {
        Server svr = (Server) ctx;
        switch (KeeperException.Code.get(rc)) {
          case CONNECTIONLOSS:
            nodesManager.removeAlert(svr);
            break;
          case OK:
            LOGGER.info("Server {} re-enabled ({})", svr.getName(), svr.getServerAddress()
                .getHostname());
            break;
          default:
            LOGGER.info("[{}] Unexpected result for alerting {} ({})", new Object[] {
                KeeperException.Code.get(rc), svr.getName(), path});
        }
      }
    };
    return removeAlertCallback;
  }


  /**
   * Check Alert create Async on zookeeper
   * 
   * @return
   */
  public static AsyncCallback.StringCallback checkCreateAlertStatusAsync() {
    AsyncCallback.StringCallback createAlertCallback = new AsyncCallback.StringCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, String name) {

        Server svr = (Server) ctx;
        switch (KeeperException.Code.get(rc)) {
          case CONNECTIONLOSS:
            nodesManager.registerAlert(svr, false);
            break;
          case NODEEXISTS:
            LOGGER.info("Trying to alert an already silenced server [" + name + "]");
            break;
          case OK:
            LOGGER.info("Server {} silenced ({})", svr.getName(), svr.getServerAddress()
                .getHostname());
            try {
              if (nodesManager.getMonitoredServers() != null) {
                LOGGER.info("STATUS :: NOW, There are currently {} " + "servers: {}", nodesManager
                    .getMonitoredServers().size(), nodesManager.getMonitoredServers().toString());
              }
            } catch (Exception e) {
              LOGGER.info("Unable to get the monitored servers");
            }
            break;
          default:
            LOGGER.info("[{}] Unexpected result for alerting {} ({})", new Object[] {
                KeeperException.Code.get(rc), svr.getName(), path});
        }
      }
    };
    return createAlertCallback;
  }

  /**
   * This method will check whether the path created yet or not. And keep of checking until created.
   * 
   * @param nodePath
   * @param signal
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void waitForSignal(String nodePath, CountDownLatch signal) throws KeeperException,
      InterruptedException {
    Stat stat = null;
    try {
      stat = zk.exists(nodePath, nodesManager);
      while (stat == null) {
        stat = zk.exists(nodePath, nodesManager);
      }
      if (signal != null) {
        LOGGER.info("SIGNAL RECIEVED FOR NODE {}", nodePath);
        signal.countDown();
        return;
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.info("Unable to check node :: " + nodePath + " Exception is :: " + e.getMessage());
      LOGGER.info(" PLEASE CHECK, ZOOKEEPER SERVER IS RUNNING or NOT!!");
    }
  }

  /**
   * @throws IOException
   */
  public static void startZookeeperServer(String jobuuid) throws IOException {
    LOGGER.info("Executing shell command to start the zookeeper");
    String zkScriptPath = Paths.get("../bin").toAbsolutePath().toString() + PathUtil.SEPARATOR_CHAR;
    String[] strArr = new String[] {"/bin/sh", zkScriptPath + "start-zk-server.sh", jobuuid};
    CommonUtil.executeScriptCommand(strArr);
    LOGGER.info("Shell command is executed");
  }

  /**
   * @throws IOException
   */
  public static String checkZookeeperServerStatus(String jobuuid) throws IOException {
    LOGGER.info("Executing shell command to check the zookeeper server status");
    String zkScriptPath = Paths.get("../bin").toAbsolutePath().toString() + PathUtil.SEPARATOR_CHAR;
    String[] strArr = new String[] {"/bin/sh", zkScriptPath + "zk-server-status.sh", jobuuid};
    String retStatus = CommonUtil.executeScriptCommand(strArr);
    LOGGER.info("Shell command is executed");
    return retStatus;
  }

  public static boolean isZkTransient(Field[] fields, int fieldIndex) {
    ZkTransient zkTransient = fields[fieldIndex].getAnnotation(ZkTransient.class);
    return (zkTransient == null) ? false : zkTransient.value();
  }

  public static boolean isZkTransient(Method[] methods, int methodIndex) {
    ZkTransient zkTransient = methods[methodIndex].getAnnotation(ZkTransient.class);
    return (zkTransient == null) ? false : zkTransient.value();
  }

  public static void createZkNode(String basePath, Object object) throws IllegalArgumentException,
      IllegalAccessException, IOException, InterruptedException, ClassNotFoundException {
    Field[] fields = object.getClass().getDeclaredFields();
    for (int index = 0; index < fields.length; index++) {
      fields[index].setAccessible(true);
      Field field = fields[index];
      Object value = FieldUtils.readField(field, object);
      if (value == null)
        continue;
      if (ClassUtils.isPrimitiveOrWrapper(field.getType())) {
        String childPath =
            basePath + File.separatorChar + field.getName() + File.separatorChar + value;
        CountDownLatch signal = new CountDownLatch(1);
        nodesManager.createPersistentNode(childPath, signal, isZkTransient(fields, index));
        signal.await();
        continue;
      }
      basePath = basePath + File.separatorChar + field.getDeclaringClass().getName();
      createZkNode(basePath, value);
    }
  }

  public static <K, V> void createZkNodeMap(String basePath, Map<K, V> map)
      throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    int id = 0;
    for (Entry<K, V> entry : map.entrySet()) {
      K key = entry.getKey();
      V value = entry.getValue();
      String kId = ("K" + id);
      String vId = ("V" + id);
      if (ClassUtils.isPrimitiveOrWrapper(key.getClass())) {
        CountDownLatch signal = new CountDownLatch(1);
        nodesManager.saveObjectZkNode(basePath, signal, key);
        signal.await();
      } else if (key instanceof Map) {
        String childPath =
            basePath + File.separatorChar + kId + File.separatorChar + key.getClass().getName();
        createZkNodeMap(childPath, (Map) key);
      } else if (key instanceof String) {
        String childPath = basePath + File.separatorChar + kId + File.separatorChar + key;
        CountDownLatch signal = new CountDownLatch(1);
        nodesManager.createPersistentNode(childPath, signal, false);
        signal.await();
      } else {
        String childPath =
            basePath + File.separatorChar + kId + File.separatorChar + key.getClass().getName();
        CountDownLatch signal = new CountDownLatch(1);
        nodesManager.saveObjectZkNode(childPath, signal, key);
        signal.await();
      }
      if (ClassUtils.isPrimitiveOrWrapper(value.getClass())) {
        CountDownLatch signal = new CountDownLatch(1);
        nodesManager.saveObjectZkNode(basePath, signal, value);
        signal.await();
      } else if (value instanceof Map) {
        String childPath =
            basePath + File.separatorChar + vId + File.separatorChar + value.getClass().getName();
        createZkNodeMap(childPath, (Map) value);
      } else if (value instanceof String) {
        String childPath = basePath + File.separatorChar + vId + File.separatorChar + value;
        CountDownLatch signal = new CountDownLatch(1);
        nodesManager.createPersistentNode((String) value, signal, false);
        signal.await();
      } else {
        String childPath =
            basePath + File.separatorChar + vId + File.separatorChar + value.getClass().getName();
        CountDownLatch signal = new CountDownLatch(1);
        nodesManager.saveObjectZkNode(childPath, signal, value);
        signal.await();
      }
      id++;
    }
  }

  public static void createZkNodeMethod(String basePath, Object object)
      throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    Method[] methods = object.getClass().getDeclaredMethods();
    for (int index = 0; index < methods.length; index++) {
      Method method = methods[index];
      Object value = method.getDefaultValue();
      if (value == null)
        continue;
      if (ClassUtils.isPrimitiveOrWrapper(method.getClass())) {
        String childPath = basePath + File.separatorChar + value;
        CountDownLatch signal = new CountDownLatch(1);
        nodesManager.createPersistentNode(childPath, signal, isZkTransient(methods, index));
        signal.await();
        continue;
      }
      basePath = basePath + File.separatorChar + method.getName();
      createZkNodeMethod(basePath, value);
    }
  }
}
