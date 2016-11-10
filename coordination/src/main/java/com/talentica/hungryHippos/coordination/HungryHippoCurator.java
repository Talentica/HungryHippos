package com.talentica.hungryHippos.coordination;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
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

import org.apache.commons.lang3.ClassUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.annotations.ZkTransient;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.ServerAddress;
import com.talentica.hungryHippos.coordination.domain.ZookeeperConfiguration;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.utility.ZkNodeName;
import com.talentica.hungryHippos.utility.PathEnum;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.config.coordination.ZookeeperDefaultConfig;

/**
 * {@code HungryHippoCurator} used for all the operations related to zookeeper.
 * 
 * @author sudarshans
 *
 */
public class HungryHippoCurator {

  private static final Logger logger = LoggerFactory.getLogger(HungryHippoCurator.class);

  private CuratorFramework curatorFramework = null;
  private static volatile HungryHippoCurator hungryHippoCurator = null;

  private Map<String, String> pathMap = new HashMap<String, String>();
  private ZookeeperConfiguration zkConfiguration;

  private List<Server> servers = new ArrayList<>();
  private Map<String, Server> serverNameMap = new HashMap<>();


  private static String ZK_ROOT_NODE = "/rootnode";


  private static String NODE_NAME_PRIFIX = "_node";
  public static String ZK_PATH_SEPERATOR = "/";


  private HungryHippoCurator() {

  }

  /**
   * creates a new HungryHippoCurator from the {@value connectString}.
   * 
   * @param connectString
   * @return a new instance HungryHippoCurator if not already instantiated.
   */
  public static HungryHippoCurator getInstance(String connectString) {
    if (hungryHippoCurator == null) {
      synchronized (HungryHippoCurator.class) {
        if (hungryHippoCurator == null) {
          hungryHippoCurator = new HungryHippoCurator();
          hungryHippoCurator.connectToZookeeper(connectString);
        }
      }

    }
    return hungryHippoCurator;
  }

  /**
   * creates a new HungryHippoCurator from the {@value connectString, sessionTimeOut}.
   * 
   * @param connectString
   * @param sessionTimeOut
   * @return a new instance HungryHippoCurator if not already instantiated.
   */
  public static HungryHippoCurator getInstance(String connectString, int sessionTimeOut) {
    if (hungryHippoCurator == null) {
      synchronized (HungryHippoCurator.class) {
        if (hungryHippoCurator == null) {
          hungryHippoCurator = new HungryHippoCurator();
          hungryHippoCurator.connectToZookeeper(connectString);
        }
      }

    }
    return hungryHippoCurator;
  }

  /**
   * 
   * @return the instance of HungryHippoCurator if already instantiated. else will return null.
   */
  public static HungryHippoCurator getInstance() {
    return hungryHippoCurator;

  }

  private void connectToZookeeper(String connectString) {
    if (this.curatorFramework == null) {
      int baseSleepTime = 10000;
      int maxRetry = 3;
      ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(baseSleepTime, maxRetry);
      this.curatorFramework = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
      start();
    }

  }



  private void start() {
    curatorFramework.start();
  }

  /**
   * creates persistent node {@value path} in zookeeper with null value inside the node.
   * 
   * @param path
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createPersistentNode(String path) throws HungryHippoException {
    String location = null;
    try {
      byte[] value = null;
      location = curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .withACL(Ids.OPEN_ACL_UNSAFE).forPath(path, value);

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

    return location;
  }

  /**
   * creates persistent node {@value path} in zookeeper with {@value data} if is a string value it
   * is inside the node. else it will create nodes recursively.
   * 
   * @param path
   * @param data
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createPersistentNode(String path, Object data) throws HungryHippoException {
    String location = null;

    try {

      if (data == null) {
        location = createPersistentNode(path);
      } else if (data != null && data instanceof String) {
        location =
            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                .withACL(Ids.OPEN_ACL_UNSAFE).forPath(path, ((String) data).getBytes());
      } else {
        saveObjectZkNode(path, data);
      }

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

    return location;
  }


  /**
   * creates persistent node {@value path} in zookeeper in asynchronous manner with null.
   * {@value callback } can be used to mention what to do if something went wrong during the async
   * call.
   * 
   * @param path
   * @param callback
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createPersistentNodeAsync(String path, StringCallback callback)
      throws HungryHippoException {
    String location = null;
    try {
      curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
          .withACL(Ids.OPEN_ACL_UNSAFE).inBackground(callback).forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return location;
  }

  /**
   * creates persistent node {@value path} in zookeeper with {@value data} if is a string value it
   * is inside the node. else it will create nodes recursively. {@value callback } can be used to
   * mention what to do if something went wrong during the async
   * 
   * @param path
   * @param data
   * @param callback
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createZnodeAsync(String path, Object data, StringCallback callback)
      throws HungryHippoException {
    String location = null;
    if (data == null) {
      createPersistentNodeAsync(path, callback);
    }
    try {
      if (data instanceof String) {
        curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
            .withACL(Ids.OPEN_ACL_UNSAFE).inBackground(callback)
            .forPath(path, ((String) data).getBytes());
      } else {
        saveObjectZkNode(path, data);;
      }
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return location;
  }

  /**
   * creates EphemeralNode node {@value path} in zookeeper with null value inside the node.
   * 
   * @param path
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createEphemeralNode(String path) throws HungryHippoException {
    String location = null;

    try {

      location = curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
          .withACL(Ids.OPEN_ACL_UNSAFE).forPath(path);

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

    return location;
  }

  /**
   * creates EphemeralNode node {@value path} in zookeeper with {@value data} if is a string value
   * it is inside the node. else it will create nodes recursively.
   * 
   * @param path
   * @param data
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createEphemeralNode(String path, Object data) throws HungryHippoException {
    String location = null;

    try {
      if (data == null) {
        location = createEphemeralNode(path);
      } else if (data != null && data instanceof String) {
        location =
            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .withACL(Ids.OPEN_ACL_UNSAFE).forPath(path, ((String) data).getBytes());
      } else {
        saveObjectZkNode(path, data);
      }


    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }


    return location;
  }

  /**
   * creates EphemeralNode node {@value path} and a sequence is appended to the path while its
   * creation in zookeeper with null value inside the node.
   * 
   * @param path
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createEphemeralNodeSeq(String path) throws HungryHippoException {
    String location = null;

    try {

      location = curatorFramework.create().creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL_SEQUENTIAL).withACL(Ids.OPEN_ACL_UNSAFE).forPath(path);

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return location;
  }

  /**
   * creates EphemeralNode node {@value path} and a sequence is appended to the path while its
   * creation in zookeeper with inside the node. {@value data} if is a string value it is inside the
   * node. else it will create nodes recursively.
   * 
   * @param path
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createEphemeralNodeSeq(String path, Object data) throws HungryHippoException {
    String location = null;

    try {
      if (data == null) {
        location = createEphemeralNodeSeq(path);
      } else if (data != null && data instanceof String) {
        location = curatorFramework.create().creatingParentsIfNeeded().withProtection()
            .withMode(CreateMode.EPHEMERAL).withACL(Ids.OPEN_ACL_UNSAFE)
            .forPath(path, ((String) data).getBytes());
      } else {
        saveObjectZkNode(path, data);
      }
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

    return location;
  }


  /**
   * creates Persistent node {@value path} and a sequence is appended to the path while its creation
   * in zookeeper with null value inside the node.
   * 
   * @param path
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */
  public String createPersistentZnodeSeq(String path) throws HungryHippoException {
    String location = null;

    try {
      location = curatorFramework.create().creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT_SEQUENTIAL).withACL(Ids.OPEN_ACL_UNSAFE).forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return location;
  }

  /**
   * creates Persistent node {@value path} and a sequence is appended to the path while its creation
   * in zookeeper with inside the node. {@value data} if is a string value it is inside the node.
   * else it will create nodes recursively.
   * 
   * @param path
   * @return a String containing the zookeeper path created.
   * @throws HungryHippoException
   */

  public String createPersistentZnodeSeq(String path, Object data) throws HungryHippoException {
    String location = null;

    try {
      if (data == null) {
        location = createEphemeralNodeSeq(path);
      } else if (data != null && data instanceof String) {
        location = curatorFramework.create().creatingParentsIfNeeded().withProtection()
            .withMode(CreateMode.PERSISTENT_SEQUENTIAL).withACL(Ids.OPEN_ACL_UNSAFE)
            .forPath(path, ((String) data).getBytes());
      } else {
        saveObjectZkNode(path, data);
      }

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

    return location;
  }


  /**
   * retrieves data inside the {@value path}.
   * 
   * @param path
   * @return a String containing the data inside the path. if not path found in zookeeper it returns
   *         null;
   * @throws HungryHippoException
   */
  public String getZnodeData(String path) throws HungryHippoException {
    byte[] byteValue;
    String data = null;

    try {
      byteValue = curatorFramework.getData().forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

    if (byteValue != null) {
      data = new String(byteValue, Charset.forName("UTF-8"));
    }

    return data;
  }

  /**
   * sets Data to the {@value path}
   * 
   * @param path
   * @param data
   * @return a Stat instance associated with this path.
   * @throws HungryHippoException
   */
  public Stat setZnodeData(String path, Object data) throws HungryHippoException {

    Stat stat = null;

    try {
      if (data == null) {
        stat = curatorFramework.setData().forPath(path);
      } else if (data instanceof String) {
        stat = curatorFramework.setData().forPath(path, ((String) data).getBytes());
      } else {
        saveObjectZkNode(path, data);
      }
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return stat;
  }

  /**
   * sets Data to the {@value path} in async manner. default callback will be used in this scenario.
   * 
   * @param path
   * @param data
   * @return a Stat instance associated with this path.
   * @throws HungryHippoException
   */
  public Stat setZnodeDataAsync(String path, Object data) throws HungryHippoException {
    Stat stat = null;
    try {

      if (data == null) {
        stat = curatorFramework.setData().inBackground().forPath(path);
      } else if (data instanceof String) {
        stat = curatorFramework.setData().inBackground().forPath(path, ((String) data).getBytes());
      } else {
        saveObjectZkNode(path, data);
      }

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return stat;
  }

  /**
   * sets Data to the {@value path} in async manner. {@value listener} can be used to handle
   * exception scenario in this case.
   * 
   * @param path
   * @param data
   * @param listener
   * @return a Stat instance associated with this path.
   * @throws HungryHippoException
   */
  public Stat setZnodeDataAsync(String path, Object data, CuratorListener listener)
      throws HungryHippoException {
    Stat stat = null;
    try {
      curatorFramework.getCuratorListenable().addListener(listener);

      if (data == null) {
        stat = curatorFramework.setData().inBackground().forPath(path);
      } else if (data instanceof String) {
        stat = curatorFramework.setData().inBackground().forPath(path, ((String) data).getBytes());
      } else {
        saveObjectZkNode(path, data);
      }

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return stat;
  }

  /**
   * 
   * @param path
   * @return a Stat instance associated with this path.
   * @throws HungryHippoException
   */
  public Stat getStat(String path) throws HungryHippoException {
    Stat stat = null;
    try {
      stat = curatorFramework.checkExists().forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return stat;
  }

  /**
   * sets Data to the {@value path} in async manner. {@value callBack} can be used to handle
   * exception scenario in this case.
   * 
   * @param callBack
   * @param path
   * @param data
   * @return
   * @throws HungryHippoException
   */
  public Stat setZnodeDataAsyncCallback(BackgroundCallback callBack, String path, String data)
      throws HungryHippoException {
    Stat stat = null;
    try {

      curatorFramework.setData().inBackground(callBack).forPath(path, data.getBytes());

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return stat;
  }

  /**
   * Check whether {@value path} is present in the zookeeper.
   * 
   * @param path
   * @return boolean. true if exits otherwise false.
   * @throws HungryHippoException
   */
  public boolean checkExists(String path) throws HungryHippoException {
    Stat stat = null;
    try {
      stat = curatorFramework.checkExists().forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return stat != null ? true : false;
  }

  /**
   * Check whether {@value path} is present in the zookeeper in asyn manner.
   * 
   * @param path
   * @return boolean. true if exits otherwise false.
   * @throws HungryHippoException
   */
  public boolean checkExistsAsync(String path) throws HungryHippoException {
    Stat stat = null;
    try {
      curatorFramework.checkExists().inBackground().forPath(path);

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return stat != null ? true : false;
  }

  /**
   * Check whether {@value path} is present in the zookeeper in async manner. {@value callback} it
   * can be used to handle the exception occured during async call.
   * 
   * @param path
   * @param callback
   * @return boolean. true if exits otherwise false.
   * @throws HungryHippoException
   */
  public boolean checkExistsAsync(String path, BackgroundCallback callback)
      throws HungryHippoException {
    Stat stat = null;
    try {
      curatorFramework.checkExists().inBackground(callback).forPath(path);

    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return stat != null ? true : false;
  }

  /**
   * Deletes the {@value path} from the zookeeper if exits. will not delete if children exits. if
   * not present throws an exception.
   * 
   * @param path
   * @throws HungryHippoException
   */
  public void delete(String path) throws HungryHippoException {

    try {
      curatorFramework.delete().forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

  }

  /**
   * Deletes the {@value path} from the zookeeper if exits. It also deletes childrens if present. if
   * not present throws an exception.
   * 
   * @param path
   * @throws HungryHippoException
   */
  public void deleteRecursive(String path) throws HungryHippoException {

    try {
      curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

  }

  /**
   * Deletes the {@value path} from the zookeeper for the edge cases of zookeeper.
   * 
   * @param path
   * @throws HungryHippoException
   */
  public void gurantedDelete(String path) throws HungryHippoException {

    try {
      curatorFramework.delete().guaranteed().forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

  }

  /**
   * Deletes the {@value path} and its children from the zookeeper for the edge cases of zookeeper.
   * 
   * @param path
   * @throws HungryHippoException
   */
  public void gurantedDeleteRecursive(String path) throws HungryHippoException {

    try {
      curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }

  }

  /**
   *
   * @param path
   * @return List<String> contains a children details associated with {@value path}
   * @throws HungryHippoException
   */
  public List<String> getChildren(String path) throws HungryHippoException {
    List<String> children = new ArrayList<>();
    try {
      children.addAll(curatorFramework.getChildren().forPath(path));
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return Collections.unmodifiableList(children);
  }

  /**
   * {@value watcher} can be used to set watcher on the {@value path} in zookeeper.
   * 
   * @param path
   * @param watcher
   * @return List<String> contains a children details associated with {@value path}
   * @throws HungryHippoException
   */
  public List<String> getChildren(String path, Watcher watcher) throws HungryHippoException {
    List<String> children = new ArrayList<>();
    try {
      children.addAll(curatorFramework.getChildren().usingWatcher(watcher).forPath(path));
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
    return Collections.unmodifiableList(children);
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

  public static final String CHARSET = "UTF8";

  /**
   * 
   * @param parentNode
   * @return Object , try to construct an object from the {@value path}.
   * @throws HungryHippoException
   */
  public Object readObject(String parentNode) throws HungryHippoException {
    try {
      String classNameParen = getChildren(parentNode).get(0);
      List<String> values = getChildren(parentNode + ZK_PATH_SEPERATOR + classNameParen);
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
          return URLDecoder.decode(valueString, Charset.forName("UTF-8").name()).charAt(0);
        case ("float"):
          return Float.valueOf(valueString);
        case ("double"):
          return Double.valueOf(valueString);
        case ("java.lang.String"):
          return URLDecoder.decode(valueString, Charset.forName("UTF-8").name());
        case ("boolean"):
          return Boolean.valueOf(valueString);
        case ("MAP"):
          Map<Object, Object> map = new HashMap<Object, Object>();

          List<String> valueStrings = getChildren(parentNode + ZK_PATH_SEPERATOR + classNameParen);
          for (String entryNum : valueStrings) {
            String keyNode = parentNode + ZK_PATH_SEPERATOR + classNameParen + ZK_PATH_SEPERATOR
                + entryNum + ZK_PATH_SEPERATOR + "key=";
            String valueNode = parentNode + ZK_PATH_SEPERATOR + classNameParen + ZK_PATH_SEPERATOR
                + entryNum + ZK_PATH_SEPERATOR + "value=";
            Object key = readObject(keyNode);
            Object value = readObject(valueNode);
            map.put(key, value);
          }
          return map;
        case ("ITERABLE"):
          List<Object> list = new LinkedList<>();
          List<String> entries = getChildren(parentNode + ZK_PATH_SEPERATOR + classNameParen);
          for (String entryNum : entries) {
            String entryString =
                parentNode + ZK_PATH_SEPERATOR + classNameParen + ZK_PATH_SEPERATOR + entryNum;
            Object value = readObject(entryString);
            list.add(value);
          }
          return list;
        case ("ARRAY"):
          String componentType =
              getChildren(parentNode + ZK_PATH_SEPERATOR + classNameParen).get(0);
          entries = getChildren(
              parentNode + ZK_PATH_SEPERATOR + classNameParen + ZK_PATH_SEPERATOR + componentType);

          Object arrayObject =
              Array.newInstance(getClassFromClassName(componentType), entries.size());
          for (String entryNum : entries) {
            String entryString = parentNode + ZK_PATH_SEPERATOR + classNameParen + ZK_PATH_SEPERATOR
                + componentType + ZK_PATH_SEPERATOR + entryNum;
            Object value = readObject(entryString);
            Array.set(arrayObject, Integer.parseInt(entryNum), value);
          }
          return arrayObject;
        default:
          Class<?> objClass = Class.forName(className);
          Object obj = objClass.newInstance();
          List<String> fieldNames = getChildren(parentNode + ZK_PATH_SEPERATOR + classNameParen);
          for (String fieldName : fieldNames) {
            Field field = objClass.getDeclaredField(fieldName.substring(0, fieldName.length() - 1));
            field.setAccessible(true);
            String valueNodeString =
                parentNode + ZK_PATH_SEPERATOR + classNameParen + ZK_PATH_SEPERATOR + fieldName;
            Object value = readObject(valueNodeString);
            field.set(obj, value);
          }
          return obj;
      }
    } catch (Exception ex) {
      throw new HungryHippoException(ex.getMessage());
    }

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

  String openBrackets = "(";
  String closedBrackets = ")/";

  /**
   * saves {@value parentNode and object} in zookeeper as znodes.
   * 
   * @param parentNode
   * @param object
   * @throws HungryHippoException
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void saveObjectZkNode(String parentNode, Object object) throws HungryHippoException {
    try {
      String className = getClassIdentifier(object);
      if (object == null) {

      } else if (object instanceof Map<?, ?>) {
        Iterable<Map.Entry> iterable = ((Map) object).entrySet();
        int index = 0;
        for (Map.Entry o : iterable) {
          saveObjectZkNode(parentNode + ZK_PATH_SEPERATOR + openBrackets + className
              + closedBrackets + (index++), new KeyValuePair(o.getKey(), o.getValue()));
        }
      } else if (object instanceof Iterable<?>) {
        Iterable<?> iterable = (Iterable<?>) object;
        int index = 0;
        for (Object o : iterable) {
          saveObjectZkNode(parentNode + ZK_PATH_SEPERATOR + openBrackets + className
              + closedBrackets + (index++), o);
        }
      } else if (object.getClass().isArray()) {
        int length = Array.getLength(object);
        for (int index = 0; index < length; index++) {
          saveObjectZkNode(
              parentNode + ZK_PATH_SEPERATOR + openBrackets + className + closedBrackets
                  + object.getClass().getComponentType().getName() + ZK_PATH_SEPERATOR + (index),
              Array.get(object, index));
        }
      } else if (object instanceof KeyValuePair) {
        saveObjectZkNode(parentNode + ZK_PATH_SEPERATOR + "key=", ((KeyValuePair) object).key);
        saveObjectZkNode(parentNode + ZK_PATH_SEPERATOR + "value=", ((KeyValuePair) object).value);
      } else if (ClassUtils.isPrimitiveOrWrapper(object.getClass())) {
        if (object instanceof Character) {
          object = URLEncoder.encode("" + object, CHARSET);
        }
        createPersistentNode(parentNode + ZK_PATH_SEPERATOR + openBrackets + className
            + closedBrackets + URLEncoder.encode(object.toString(), CHARSET));
      } else if (object instanceof String) {
        createPersistentNode(parentNode + ZK_PATH_SEPERATOR + openBrackets + className
            + closedBrackets + URLEncoder.encode(object.toString(), CHARSET));
      } else {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
          if (isZkTransient(field) || ((field.getModifiers() & Modifier.STATIC) > 0)) {
            continue;
          }
          field.setAccessible(true);
          String fieldName = field.getName();
          Object value;
          value = field.get(object);
          String fieldNameString = fieldName + ZkNodeName.EQUAL.getName();
          saveObjectZkNode(parentNode + ZK_PATH_SEPERATOR + openBrackets + className
              + closedBrackets + fieldNameString, value);
        }
      }
    } catch (Exception e) {
      throw new HungryHippoException(e.getMessage());
    }
  }

  private String getClassIdentifier(Object c) {
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

  /**
   * @param field
   * @return boolean, true if a field is annotated with ZkTransient. otherwise false.
   */
  public boolean isZkTransient(Field field) {
    ZkTransient zkTransient = field.getAnnotation(ZkTransient.class);
    return (zkTransient == null) ? false : zkTransient.value();
  }

  /**
   * 
   * @param methods
   * @param methodIndex
   * @return boolean, true if a field is annotated with ZkTransient. otherwise false.
   */
  public boolean isZkTransient(Method[] methods, int methodIndex) {
    ZkTransient zkTransient = methods[methodIndex].getAnnotation(ZkTransient.class);
    return (zkTransient == null) ? false : zkTransient.value();
  }

  /**
   * buildNodePath
   * 
   * @param nodeId
   * @return
   */
  public String buildNodePath(int nodeId) {
    return CoordinationConfigUtil.getZkCoordinationConfigCache().getZookeeperDefaultConfig()
        .getHostPath() + ZK_PATH_SEPERATOR + ("_node" + nodeId);
  }

  public void initializeZookeeperDefaultConfig(ZookeeperDefaultConfig zookeeperDefaultConfig) {
    pathMap.put(PathEnum.NAMESPACE.name(), zookeeperDefaultConfig.getNamespacePath());
    pathMap.put(PathEnum.BASEPATH.name(), zookeeperDefaultConfig.getHostPath());
    pathMap.put(PathEnum.ALERTPATH.name(), zookeeperDefaultConfig.getAlertPath());
    pathMap.put(PathEnum.CONFIGPATH.name(),
        CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path"));
    pathMap.put(PathEnum.FILESYSTEM.name(), zookeeperDefaultConfig.getFilesystemPath());
    pathMap.put(PathEnum.SHARDING_TABLE.name(), zookeeperDefaultConfig.getShardingTablePath());
    pathMap.put(PathEnum.JOB_CONFIG.name(), zookeeperDefaultConfig.getJobConfigPath());
    pathMap.put(PathEnum.JOB_STATUS.name(), zookeeperDefaultConfig.getJobStatusPath());
    pathMap.put(PathEnum.COMPLETED_JOBS.name(), zookeeperDefaultConfig.getJobStatusPath()
        + ZK_PATH_SEPERATOR + PathEnum.COMPLETED_JOBS.getPathName());
    pathMap.put(PathEnum.FAILED_JOBS.name(), zookeeperDefaultConfig.getJobStatusPath()
        + ZK_PATH_SEPERATOR + PathEnum.FAILED_JOBS.getPathName());
    pathMap.put(PathEnum.STARTED_JOB_ENTITY.name(), zookeeperDefaultConfig.getJobStatusPath()
        + ZK_PATH_SEPERATOR + PathEnum.STARTED_JOB_ENTITY.getPathName());
    pathMap.put(PathEnum.COMPLETED_JOB_ENTITY.name(), zookeeperDefaultConfig.getJobStatusPath()
        + ZK_PATH_SEPERATOR + PathEnum.COMPLETED_JOB_ENTITY.getPathName());
    pathMap.put(PathEnum.PENDING_JOBS.name(), zookeeperDefaultConfig.getJobStatusPath()
        + ZK_PATH_SEPERATOR + PathEnum.PENDING_JOBS.getPathName());
    pathMap.put(PathEnum.IN_PROGRESS_JOBS.name(), zookeeperDefaultConfig.getJobStatusPath()
        + ZK_PATH_SEPERATOR + PathEnum.IN_PROGRESS_JOBS.getPathName());
    pathMap.put(PathEnum.COMPLETED_JOB_NODES.name(), zookeeperDefaultConfig.getJobStatusPath()
        + ZK_PATH_SEPERATOR + PathEnum.COMPLETED_JOB_NODES.getPathName());
    pathMap.put(PathEnum.FAILED_JOB_NODES.name(), zookeeperDefaultConfig.getJobStatusPath()
        + ZK_PATH_SEPERATOR + PathEnum.FAILED_JOB_NODES.getPathName());
    pathMap.put(PathEnum.FILEID_HHFS_MAP.name(), zookeeperDefaultConfig.getFileidHhfsMapPath());

    zkConfiguration = new ZookeeperConfiguration(pathMap);

  }


  public void cleanUpZkNodeRecursively() throws Exception, InterruptedException {

    hungryHippoCurator.gurantedDeleteRecursive(pathMap.get(PathEnum.NAMESPACE.name()));

  }

  /**
   * To get monitored server
   * 
   * @return
   * @throws HungryHippoException
   */
  public List<String> getMonitoredServers() throws HungryHippoException {
    List<String> serverList =
        hungryHippoCurator.getChildren(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()));
    Collections.sort(serverList);
    return serverList;
  }

  /**
   * Build path for Alert
   * 
   * @param server
   * @return String
   */
  protected String buildAlertPathForServer(Server server) {
    return buildAlertPathForServer(server.getServerAddress().getHostname());
  }

  /**
   * @param serverHostname
   * @return string
   */
  protected String buildAlertPathForServer(String serverHostname) {
    return zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()) + ZK_PATH_SEPERATOR
        + serverHostname;
  }

  /**
   * 
   * @param nodeName
   * @return String whichc creates alertPath to the node.
   */
  public String buildAlertPathByName(String nodeName) {
    return zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()) + ZK_PATH_SEPERATOR
        + nodeName;
  }

  /**
   * Create the servers map which all need to be run on nodes
   * 
   */
  private void createServersMap() {
    List<String> checkUnique = new ArrayList<String>();
    List<Node> nodes;
    nodes = CoordinationConfigUtil.getLocalClusterConfig().getNode();

    for (int index = 0; index < nodes.size(); index++) {
      Node node = nodes.get(index);
      if (!checkUnique.contains(node.getIp())) {
        Server server = new Server();
        server.setServerAddress(new ServerAddress(NODE_NAME_PRIFIX + index, node.getIp()));
        server.setData(new Date().getTime());
        server.setServerType("simpleserver");
        server.setCurrentDateTime(getCurrentTimeStamp());
        server.setDescription("A simple server to test monitoring");
        server.setId(index);
        this.servers.add(server);
        checkUnique.add(node.getIp());
        serverNameMap.put(server.getServerAddress().getHostname(), server);

      }

    }
  }

  /**
   * Get current timestamp in format yyyy-MM-dd_HH:mm:ss
   * 
   * @return
   */
  private String getCurrentTimeStamp() {
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
    Date now = new Date();
    String strDate = sdfDate.format(now);
    return strDate;
  }

  /**
   * To start the application. Once the connection is established, this method register the servers
   * on the zookeeper nodes which will be under monitoring throughout the live application.
   * 
   * @throws Exception
   */
  public void startup() throws HungryHippoException {
    String nameSpacePath = pathMap.get(PathEnum.NAMESPACE.name());
    String configPath = pathMap.get(PathEnum.CONFIGPATH.name());

    cleanUp(nameSpacePath);
    cleanUp(configPath);
    cleanUp("/torrent"); // TODO need to remove hardcoded value.

    defaultNodesOnStart();
  }

  private void cleanUp(String path) throws HungryHippoException {
    if (hungryHippoCurator.checkExists(path)) {
      hungryHippoCurator.gurantedDeleteRecursive(path);
      logger.info("Deleted {}", path);
    }

  }

  /**
   * Bootstrap
   * 
   * @throws HungryHippoException
   */
  public void defaultNodesOnStart() throws HungryHippoException {
    createServersMap();
    // TODO populate from map using loop.
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.NAMESPACE.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.CONFIGPATH.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.FILESYSTEM.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.SHARDING_TABLE.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.JOB_CONFIG.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.JOB_STATUS.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.COMPLETED_JOBS.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.FAILED_JOBS.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.STARTED_JOB_ENTITY.name()));
    hungryHippoCurator.createPersistentNode(
        zkConfiguration.getPathMap().get(PathEnum.COMPLETED_JOB_ENTITY.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.PENDING_JOBS.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.IN_PROGRESS_JOBS.name()));
    hungryHippoCurator.createPersistentNode(
        zkConfiguration.getPathMap().get(PathEnum.COMPLETED_JOB_NODES.name()));
    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.FAILED_JOB_NODES.name()));

    hungryHippoCurator
        .createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.FILEID_HHFS_MAP.name()));

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
  public Set<LeafBean> searchLeafNode(String searchString, String authRole)
      throws InterruptedException, KeeperException, IOException, ClassNotFoundException,
      HungryHippoException {
    // LOGGER.info("IN searchTree path {}", searchString);
    /* Export all nodes and then search. */
    if (searchString.contains(ZK_PATH_SEPERATOR))
      ZK_ROOT_NODE = searchString;
    Set<LeafBean> searchResult = new TreeSet<>();
    Set<LeafBean> leaves = new TreeSet<>();
    exportTreeInternal(leaves, ZK_ROOT_NODE, authRole);
    for (LeafBean leaf : leaves) {
      if (leaf.getPath().contains(searchString) || leaf.getName().contains(searchString)) {
        searchResult.add(leaf);
      }
    }

    return searchResult;

  }

  private void exportTreeInternal(Set<LeafBean> entries, String path, String authRole)
      throws InterruptedException, KeeperException, ClassNotFoundException, IOException,
      HungryHippoException {

    entries.addAll(listLeaves(path, authRole)); // List leaves

    /* Process folders */
    for (String folder : listFolders(path)) {
      exportTreeInternal(entries, getNodePath(path, folder), authRole);
    }
  }

  /**
   * List all the last node that can be reached from {@value path}.
   * 
   * @param path
   * @param authRole
   * @return List<LeafBean> , contains the last children details.
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws HungryHippoException
   */
  public List<LeafBean> listLeaves(String path, String authRole) throws InterruptedException,
      KeeperException, ClassNotFoundException, IOException, HungryHippoException {
    List<LeafBean> leaves = new ArrayList<>();

    List<String> children = hungryHippoCurator.getChildren(path);
    if (children != null) {
      for (String child : children) {
        String childPath = getNodePath(path, child);
        List<String> subChildren = Collections.emptyList();
        subChildren = hungryHippoCurator.getChildren(childPath);
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

  /**
   * Lists all the sub childrens and its childrens.
   * 
   * @param path
   * @return List<String>, contains all the subchildren of {@value path} and also subchildren's
   *         children and so on.
   * @throws KeeperException
   * @throws InterruptedException
   * @throws HungryHippoException
   */
  public List<String> listFolders(String path)
      throws KeeperException, InterruptedException, HungryHippoException {
    List<String> folders = new ArrayList<>();
    List<String> children = hungryHippoCurator.getChildren(path);
    if (children != null) {
      for (String child : children) {
        List<String> subChildren = hungryHippoCurator.getChildren(path + ZK_PATH_SEPERATOR + child);
        boolean isFolder = subChildren != null && !subChildren.isEmpty();
        if (isFolder) {
          folders.add(child);
        }
      }
    }

    Collections.sort(folders);
    return folders;
  }

  /**
   * combines path and the name.
   * 
   * @param path
   * @param name
   * @return a String , containing the combined path details.
   */
  public String getNodePath(String path, String name) {
    return path + ZK_PATH_SEPERATOR + name;

  }

  /**
   * 
   * @param path
   * @param childPath
   * @param child
   * @param authRole
   * @return a LeafBean instance.
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws HungryHippoException
   */
  public LeafBean getNodeValue(String path, String childPath, String child, String authRole)
      throws ClassNotFoundException, IOException, KeeperException, InterruptedException,
      HungryHippoException {
    return getNodeDetail(path, childPath, child, authRole, true);
  }

  /**
   * 
   * @param path
   * @param childPath
   * @param child
   * @param authRole
   * @param getData
   * @return a LeafBean instance.
   * @throws KeeperException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws HungryHippoException
   */
  public LeafBean getNodeDetail(String path, String childPath, String child, String authRole,
      boolean getData) throws KeeperException, InterruptedException, ClassNotFoundException,
      IOException, HungryHippoException {

    byte[] dataBytes = null;
    boolean stat = hungryHippoCurator.checkExists(childPath);
    if (stat && getData) {
      dataBytes = hungryHippoCurator.getZnodeData(childPath).getBytes();
    }
    return (new LeafBean(path, child, dataBytes));

  }

  /**
   * creates the path where configuration files are stored.
   * 
   * @param fileName
   * @return a String , which contains the location where the configuration file is located.
   */
  public String buildConfigPath(String fileName) {
    String buildPath =
        zkConfiguration.getPathMap().get(PathEnum.CONFIGPATH.name()) + ZK_PATH_SEPERATOR + fileName;
    return buildPath;
  }

  public String createPersistentNodeIfNotPresent(String path) throws HungryHippoException {
    return createPersistentNodeIfNotPresent(path, null);
  }

  /**
   * checks whether {@value path} exits in zookeeper before creating.
   * 
   * @param path
   * @return
   * @throws HungryHippoException
   */
  public String createPersistentNodeIfNotPresent(String path, Object data) {
    String location = null;
    try {
      location = createPersistentNode(path, data);
    } catch (HungryHippoException e) {
      logger.warn(e.getMessage());
    }
    return location;
  }

  public void updatePersistentNode(String path, Object data) throws HungryHippoException {
    deletePersistentNodeRecursiveIfExits(path);
    createPersistentNodeIfNotPresent(path, data);

  }



  /**
   * checks whether {@value path} exits in zookeeper before deletion.
   * 
   * @param path
   * @throws HungryHippoException
   */
  public void deletePersistentNodeIfExits(String path) throws HungryHippoException {
    if (checkExists(path)) {
      delete(path);
    }
  }

  /**
   * checks whether {@value path} exits in zookeeper before deletion.
   * 
   * @param path
   * @throws HungryHippoException
   */
  public void deletePersistentNodeRecursiveIfExits(String path) throws HungryHippoException {
    if (checkExists(path)) {
      deleteRecursive(path);
    }
  }
}
