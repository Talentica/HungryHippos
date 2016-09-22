/**
 * 
 */
package com.talentica.hungryHippos.coordination;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.annotations.ZkTransient;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkNodeName;
import com.talentica.hungryHippos.utility.FileSystemConstants;
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
  public static final String zkPathSeparator = "/";
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class.getName());
  private static String ZK_ROOT_NODE = "/rootnode";
  public static ZooKeeper zk;
  public static NodesManager nodesManager;
  public static final String CHARSET = "UTF8";

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
      CountDownLatch signal)
      throws InterruptedException, KeeperException, IOException, ClassNotFoundException {
    LOGGER.info("IN searchTree path {}", searchString);
    /* Export all nodes and then search. */
    if (searchString.contains(zkPathSeparator))
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
      if (!path.equals(zkPathSeparator)) {
        childPath = path + zkPathSeparator + child;
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

  public static LeafBean getNodeDetail(String path, String childPath, String child, String authRole,
      boolean getData)
      throws KeeperException, InterruptedException, ClassNotFoundException, IOException {
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



  public static String buildNodePath(int nodeId) {
    return CoordinationConfigUtil.getZkCoordinationConfigCache().getZookeeperDefaultConfig()
        .getHostPath() + PathUtil.SEPARATOR_CHAR + ("_node" + nodeId);
  }

  public static String buildNodePath(String jobuuid) {
    return CoordinationConfigUtil.getZkCoordinationConfigCache().getZookeeperDefaultConfig()
        .getHostPath() + PathUtil.SEPARATOR_CHAR + (jobuuid);
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
      if (!nodesManager.checkNodeExists(node)) {
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
            LOGGER.info(
                "ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO CHECK STATUS...");
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
            LOGGER.info(
                "ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
            break;
          case OK:
            LOGGER.info("Node {} is  ({})", node, path);
            break;
          default:
            LOGGER.info("[{}] Unexpected result for deleting {} ({})",
                new Object[] {KeeperException.Code.get(rc), node, path});
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
            LOGGER.info(
                "ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO CHECK STATUS...");
            nodesManager.checkZookeeperConnection(svr);
            break;
          case OK:
            LOGGER.info("ZOOKEEPER SERVER IS RUNNING...");
            LOGGER.info("Node {} is  ({})", svr.getName(), svr.getServerAddress().getHostname());
            break;
          default:
            LOGGER.info("[{}] Unexpected result for STATUS {} ({})",
                new Object[] {KeeperException.Code.get(rc), svr.getName(), path});
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
            LOGGER.info(
                "ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
            break;
          case OK:
            LOGGER.info("Node {} is  ({})", svr.getName(), svr.getServerAddress().getHostname());
            break;
          default:
            LOGGER.info("[{}] Unexpected result for deleting {} ({})",
                new Object[] {KeeperException.Code.get(rc), svr.getName(), path});
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
            LOGGER.info("Server {} re-enabled ({})", svr.getName(),
                svr.getServerAddress().getHostname());
            break;
          default:
            LOGGER.info("[{}] Unexpected result for alerting {} ({})",
                new Object[] {KeeperException.Code.get(rc), svr.getName(), path});
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
            LOGGER.info("Server {} silenced ({})", svr.getName(),
                svr.getServerAddress().getHostname());
            try {
              if (nodesManager.getMonitoredServers() != null) {
                LOGGER.info("STATUS :: NOW, There are currently {} " + "servers: {}",
                    nodesManager.getMonitoredServers().size(),
                    nodesManager.getMonitoredServers().toString());
              }
            } catch (Exception e) {
              LOGGER.info("Unable to get the monitored servers");
            }
            break;
          default:
            LOGGER.info("[{}] Unexpected result for alerting {} ({})",
                new Object[] {KeeperException.Code.get(rc), svr.getName(), path});
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
  public static void waitForSignal(String nodePath, CountDownLatch signal)
      throws KeeperException, InterruptedException {
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

  private static class KeyValuePair {
    private Object key;
    private Object value;

    public KeyValuePair(Object key, Object value) {
      super();
      this.key = key;
      this.value = value;
    }
  }

  public static Object readObjectZkNode(String parentNode) {
    try {
      String classNameParen = nodesManager.getChildren(parentNode).get(0);
      List<String> values = nodesManager.getChildren(parentNode + zkPathSeparator + classNameParen);
      String valueString = values.size() > 0 ? values.get(0) : null;
      String className = classNameParen.substring(1, classNameParen.length() - 1);
      switch (className) {
        case ("int"):
          return Integer.valueOf(valueString);
        case ("long"):
          return Long.valueOf(valueString);
        case ("short"):
          return Short.valueOf(valueString);
        case ("byte"):
          return Byte.valueOf(valueString);
        case ("char"):
          return URLDecoder.decode(valueString, CHARSET).charAt(0);
        case ("float"):
          return Float.valueOf(valueString);
        case ("double"):
          return Double.valueOf(valueString);
        case ("java.lang.String"):
          return URLDecoder.decode(valueString, CHARSET);
        case ("boolean"):
          return Boolean.valueOf(valueString);
        case ("MAP"):
          Map<Object, Object> map = new HashMap<Object, Object>();
          List<String> valueStrings =
              nodesManager.getChildren(parentNode + zkPathSeparator + classNameParen);
          for (String entryNum : valueStrings) {
            String keyNode = parentNode + zkPathSeparator + classNameParen + zkPathSeparator
                + entryNum + zkPathSeparator + "key=";
            String valueNode = parentNode + zkPathSeparator + classNameParen + zkPathSeparator
                + entryNum + zkPathSeparator + "value=";
            Object key = readObjectZkNode(keyNode);
            Object value = readObjectZkNode(valueNode);
            map.put(key, value);
          }
          return map;
        case ("ITERABLE"):
          List<Object> list = new LinkedList<Object>();
          List<String> entries =
              nodesManager.getChildren(parentNode + zkPathSeparator + classNameParen);
          for (String entryNum : entries) {
            String entryString =
                parentNode + zkPathSeparator + classNameParen + zkPathSeparator + entryNum;
            Object value = readObjectZkNode(entryString);
            list.add(value);
          }
          return list;
        case ("ARRAY"):
          String componentType =
              nodesManager.getChildren(parentNode + zkPathSeparator + classNameParen).get(0);
          entries = nodesManager.getChildren(
              parentNode + zkPathSeparator + classNameParen + zkPathSeparator + componentType);

          Object arrayObject =
              Array.newInstance(getClassFromClassName(componentType), entries.size());
          for (String entryNum : entries) {
            String entryString = parentNode + zkPathSeparator + classNameParen + zkPathSeparator
                + componentType + zkPathSeparator + entryNum;
            Object value = readObjectZkNode(entryString);
            Array.set(arrayObject, Integer.parseInt(entryNum), value);
          }
          return arrayObject;
        default:
          Class<?> objClass = Class.forName(className);
          Object obj = objClass.newInstance();
          List<String> fieldNames =
              nodesManager.getChildren(parentNode + zkPathSeparator + classNameParen);
          for (String fieldName : fieldNames) {
            Field field = objClass.getDeclaredField(fieldName.substring(0, fieldName.length() - 1));
            field.setAccessible(true);
            String valueNodeString =
                parentNode + zkPathSeparator + classNameParen + zkPathSeparator + fieldName;
            Object value = readObjectZkNode(valueNodeString);
            field.set(obj, value);
          }
          return obj;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  private static Class<?> getClassFromClassName(String className) throws ClassNotFoundException {
    if (className.startsWith("[")) {
      Array.newInstance(getClassFromClassName(className.substring(1, className.length() - 1)), 0)
          .getClass();
    } else {
      switch (className) {
        case "byte":
          return byte.class;
        case "short":
          return short.class;
        case "int":
          return int.class;
        case "long":
          return long.class;
        case "float":
          return float.class;
        case "double":
          return double.class;
        case "char":
          return char.class;
        default:
          return Class.forName(className);
      }
    }
    return null;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void saveObjectZkNode(String parentNode, Object object) {
    try {
      String className = getClassIdentifier(object);
      if (object == null) {

      } else if (object instanceof Map<?, ?>) {
        Iterable<Map.Entry> iterable = ((Map) object).entrySet();
        int index = 0;
        for (Map.Entry o : iterable) {
          saveObjectZkNode(parentNode + zkPathSeparator + "(" + className + ")/" + (index++),
              new KeyValuePair(o.getKey(), o.getValue()));
        }
      } else if (object instanceof Iterable<?>) {
        Iterable<?> iterable = (Iterable<?>) object;
        int index = 0;
        for (Object o : iterable) {
          saveObjectZkNode(parentNode + zkPathSeparator + "(" + className + ")/" + (index++), o);
        }
      } else if (object.getClass().isArray()) {
        int length = Array.getLength(object);
        for (int index = 0; index < length; index++) {
          saveObjectZkNode(
              parentNode + zkPathSeparator + "(" + className + ")" + zkPathSeparator
                  + object.getClass().getComponentType().getName() + zkPathSeparator + (index),
              Array.get(object, index));
        }
      } else if (object instanceof KeyValuePair) {
        saveObjectZkNode(parentNode + zkPathSeparator + "key=", ((KeyValuePair) object).key);
        saveObjectZkNode(parentNode + zkPathSeparator + "value=", ((KeyValuePair) object).value);
      } else if (ClassUtils.isPrimitiveOrWrapper(object.getClass())) {
        if (object instanceof Character) {
          object = URLEncoder.encode("" + object, CHARSET);
        }
        nodesManager.createPersistentNodeSync(parentNode + zkPathSeparator + "(" + className + ")/"
            + URLEncoder.encode(object.toString(), CHARSET));
      } else if (object instanceof String) {
        nodesManager.createPersistentNodeSync(parentNode + zkPathSeparator + "(" + className + ")/"
            + URLEncoder.encode(object.toString(), CHARSET));
      } else {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
          if (ZkUtils.isZkTransient(field) || ((field.getModifiers() & Modifier.STATIC) > 0)) {
            continue;
          }
          field.setAccessible(true);
          String fieldName = field.getName();
          Object value;
          value = field.get(object);
          String fieldNameString = fieldName + ZkNodeName.EQUAL.getName();
          saveObjectZkNode(parentNode + zkPathSeparator + "(" + className + ")" + zkPathSeparator
              + fieldNameString, value);
        }
      }
    } catch (IllegalArgumentException | IllegalAccessException | IOException
        | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static String getClassIdentifier(Object c) {
    if (c instanceof Iterable) {
      return "ITERABLE";
    } else if (c.getClass().isArray()) {
      return "ARRAY";
    } else if (c instanceof Map) {
      return "MAP";
    } else if (c instanceof KeyValuePair) {
      return "KEYVALUE";
    } else if (ClassUtils.isPrimitiveWrapper(c.getClass())) {
      return ClassUtils.wrappersToPrimitives(c.getClass())[0].getName();
    } else {
      return c.getClass().getName();
    }
  }

  public static boolean isZkTransient(Field field) {
    ZkTransient zkTransient = field.getAnnotation(ZkTransient.class);
    return (zkTransient == null) ? false : zkTransient.value();
  }

  public static boolean isZkTransient(Method[] methods, int methodIndex) {
    ZkTransient zkTransient = methods[methodIndex].getAnnotation(ZkTransient.class);
    return (zkTransient == null) ? false : zkTransient.value();
  }

  public static String getChildrenRecursive(String valuePath, List<String> children)
      throws KeeperException, InterruptedException {
    List<String> valueChild = nodesManager.getChildren(valuePath);
    if (valueChild.size() == 1) {
      valuePath = valuePath + File.separatorChar + valueChild.get(0);
      getChildrenRecursive(valuePath, children);
    } else {
      children.addAll(valueChild);
      return valuePath;
    }
    return valuePath;
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

  /**
   * Returns list of child nodes
   * 
   * @param parentNode
   * @return
   */
  public static List<String> getChildren(String parentNode) {
    List<String> stringList = new ArrayList<>();
    try {
      NodesManager manager = NodesManagerContext.getNodesManagerInstance();
      stringList = manager.getChildren(parentNode);
    } catch (FileNotFoundException | JAXBException | KeeperException | InterruptedException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
    return stringList;
  }

  /**
   * Creates fresh Zookeeper node with data
   * 
   * @param node
   * @param data
   */
  public static void createZKNode(String node, Object data) {
    try {
      NodesManager manager = NodesManagerContext.getNodesManagerInstance();
      if (manager.checkNodeExists(node)) {
        manager.deleteNode(node);
      }
      CountDownLatch signal = new CountDownLatch(1);
      manager.createPersistentNode(node, signal, data);
      signal.await();
    } catch (IOException | JAXBException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates Zookeeper node if the it doesn't exist
   * 
   * @param node
   * @param data
   */
  public static void createZKNodeIfNotPresent(String node, Object data) {
    try {
      NodesManager manager = NodesManagerContext.getNodesManagerInstance();
      if (manager.checkNodeExists(node)) {
        return;
      }
      CountDownLatch signal = new CountDownLatch(1);
      manager.createPersistentNode(node, signal, data);
      signal.await();
    } catch (IOException | JAXBException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks if node is already present
   * 
   * @param node
   */
  public static boolean checkIfNodeExists(String node) {
    try {
      NodesManager manager = NodesManagerContext.getNodesManagerInstance();
      return manager.checkNodeExists(node);
    } catch (IOException | JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deletes Zookeeper node if it exists
   * 
   * @param node
   */
  public static void deleteZKNode(String node) {
    try {
      NodesManager manager = NodesManagerContext.getNodesManagerInstance();
      if (!manager.checkNodeExists(node)) {
        return;
      }
      manager.deleteNode(node);
    } catch (IOException | JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns String data from config node
   * 
   * @param node
   * @return
   */
  public static Object getNodeData(String node) {
    try {
      NodesManager manager = NodesManagerContext.getNodesManagerInstance();
      Object configValue = manager.getObjectFromZKNode(node);
      return configValue;
    } catch (IOException | KeeperException | InterruptedException | JAXBException
        | ClassNotFoundException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates fresh Zookeeper file node
   * 
   * @param node
   */
  public static void createFileNode(String node) {
    try {
      NodesManager manager = NodesManagerContext.getNodesManagerInstance();
      if (manager.checkNodeExists(node)) {
        manager.deleteNode(node);
      }
      CountDownLatch signal = new CountDownLatch(1);
      manager.createPersistentNode(node, signal, FileSystemConstants.IS_A_FILE);
      signal.await();
    } catch (IOException | JAXBException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * To save the object which contains only the primitive data type as instance variable. The
   * hierarchy is managed by the parent node followed by the instance variable and it corresponding
   * value i.e (id=0) or (size = 1) as complete node name.
   * 
   * @param parentNode is the node which is basically contains all the values of the object as
   *        children of it.
   * @param signal is to ensure that all the relevant children should be created as node.
   * @param object is to save onto zookeeper as a part of node formation.
   * @throws IOException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InterruptedException
   * @throws JAXBException
   */
  public void saveObjectZkNode(String parentNode, CountDownLatch signal, Object object)
      throws IOException, IllegalArgumentException, IllegalAccessException, InterruptedException,
      JAXBException {
    NodesManager manager = NodesManagerContext.getNodesManagerInstance();
    Field[] fields = object.getClass().getDeclaredFields();
    CountDownLatch counter = new CountDownLatch(fields.length);
    for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
      fields[fieldIndex].setAccessible(true);
      if (ZkUtils.isZkTransient(fields[fieldIndex])) {
        continue;
      }
      String fieldName = fields[fieldIndex].getName();
      Object value = fields[fieldIndex].get(object);
      String leafNode = fieldName + ZkNodeName.EQUAL.getName() + value;
      String leafNodePath = parentNode + File.separatorChar + leafNode;
      manager.createPersistentNodeSync(leafNodePath);
    }
    counter.await();
    signal.countDown();
  }

  private static Map<String, Boolean> cacheNodeExists;

  public static boolean checkNodeExists(String node) {
    return cacheNodeExists.get(node);
  }



  public static StatCallback checkStatNodeExists(CountDownLatch signal) {
    if (cacheNodeExists == null) {
      cacheNodeExists = new HashMap<>();
    }
    return new StatCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, Stat stat) {
        switch (KeeperException.Code.get(rc)) {
          case CONNECTIONLOSS:
            nodesManager.asyncNodeExists(path, signal);
            break;
          case NODEEXISTS:
            cacheNodeExists.put(path, (stat != null));
            signal.countDown();
            LOGGER.info("Node exists {}", path);
            break;
          case NONODE:
            cacheNodeExists.put(path, (stat != null));
            signal.countDown();
            LOGGER.info("Node does not exists {}", path);
            break;
          case OK:
            cacheNodeExists.put(path, (stat != null));
            signal.countDown();
            LOGGER.info("ZK connection established.");
            break;
          default:
            LOGGER.info("Unable to do check the stat of the node.");
        }
      }
    };

  }

}
