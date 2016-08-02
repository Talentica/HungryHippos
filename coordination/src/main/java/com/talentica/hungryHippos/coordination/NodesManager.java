/**
 * 
 */
package com.talentica.hungryHippos.coordination;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.curator.utils.ZKPaths;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.ServerAddress;
import com.talentica.hungryHippos.coordination.domain.Status;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.domain.ZookeeperConfiguration;
import com.talentica.hungryHippos.coordination.listeners.AlertManager;
import com.talentica.hungryHippos.coordination.listeners.EvictionListener;
import com.talentica.hungryHippos.coordination.listeners.RegistrationListener;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkNodeName;
import com.talentica.hungryHippos.utility.PathEnum;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.config.coordination.ZookeeperDefaultConfig;

/**
 * To manage the different nodes of the server
 * 
 * @author PooshanS
 *
 */
public class NodesManager implements Watcher {

  private static final String REMOVED = "REMOVED";
  private static final String ADDED = "ADDED";
  private org.apache.zookeeper.ZooKeeper zk;
  private CountDownLatch connectedSignal;
  private CountDownLatch getSignal;
  private EvictionListener evictionListener;
  private RegistrationListener registrationListener;
  private ZookeeperConfiguration zkConfiguration;
  private List<Server> servers;
  private Map<String, Server> serverNameMap;
  private static final Logger LOGGER = LoggerFactory.getLogger(NodesManager.class.getName());

  private static final int DEFAULT_NODES = PathEnum.values().length - 1; // subtracting
  // -1
  // because
  // HOSTPATH
  // is
  // separately
  // created.
  // Refer
  // bootstrap()
  // method.

  private Map<String, String> pathMap;
  private static final String NODE_NAME_PRIFIX = "_node";

  public RegistrationListener getRegistrationListener() {
    return registrationListener;
  }

  public void setRegistrationListener(RegistrationListener registrationListener) {
    this.registrationListener = registrationListener;
  }

  public List<Server> getServers() {
    return this.servers;
  }

  public void setServers(List<Server> servers) {
    this.servers = servers;
  }

  public NodesManager() {
    connectedSignal = new CountDownLatch(1);
    registrationListener = new AlertManager();
    evictionListener = (EvictionListener) registrationListener;
    servers = new ArrayList<Server>();
    serverNameMap = new HashMap<String, Server>();
    pathMap = new HashMap<String, String>();
  }

  /**
   * To connect to the zookeeper.
   * 
   * @param zkIP is the zookeeper ip that will be used to connected.
   * @return NodesManager
   */
  public NodesManager connectZookeeper(String zkIP, int sessionTimeout) {
    try {
      if (zkIP == null) {
        LOGGER.info("Zookeeper IP is NOT provided. Please pass the zookeeper IP's as string");
        return null;
      } else {
        connect(zkIP, sessionTimeout);
      }
      LOGGER.info("Connected - Session ID: " + zk.getSessionId());
    } catch (Exception e) {
      LOGGER.info("Could not connect to Zookeper instance" + e);
    }
    return this;
  }

  /**
   * To connect the with zookeeper host.
   * 
   * @param hosts
   * @throws Exception
   */
  private void connect(String hosts, int sessionTimeout) throws Exception {
    zk = new org.apache.zookeeper.ZooKeeper(hosts, sessionTimeout, this);
    ZkUtils.zk = zk;
    ZkUtils.nodesManager = this;
    connectedSignal.await();
  }

  public ZookeeperConfiguration initializeZookeeperDefaultConfig(
      ZookeeperDefaultConfig zookeeperDefaultConfig) {
    pathMap.put(PathEnum.NAMESPACE.name(), zookeeperDefaultConfig.getNamespacePath());
    pathMap.put(PathEnum.BASEPATH.name(), zookeeperDefaultConfig.getHostPath());
    pathMap.put(PathEnum.ALERTPATH.name(), zookeeperDefaultConfig.getAlertPath());
    pathMap.put(PathEnum.CONFIGPATH.name(),
        CoordinationApplicationContext.getProperty().getValueByKey("zookeeper.config_path"));
    pathMap.put(PathEnum.FILESYSTEM.name(), zookeeperDefaultConfig.getFilesystemPath());
    pathMap.put(PathEnum.SHARDING_TABLE.name(), zookeeperDefaultConfig.getShardingTablePath());
    zkConfiguration = new ZookeeperConfiguration(pathMap);
    return zkConfiguration;
  }

  /**
   * To start the application. Once the connection is established, this method register the servers
   * on the zookeeper nodes which will be under monitoring throughout the live application.
   * 
   * @throws Exception
   */
  public void startup() throws Exception {
    CountDownLatch signal = new CountDownLatch(1);
    ZkUtils.deleteRecursive(PathUtil.SEPARATOR_CHAR + pathMap.get(PathEnum.NAMESPACE.name()),
        signal);
    signal.await();
    defaultNodesOnStart();
    try {
      List<String> serverNames = getMonitoredServers();
      LOGGER.info("Starting Node Manager.. Already the server/servers {} running!", serverNames);
      for (String name : serverNames) {
        Server server = getServerInfo(name);
        if (server != null)
          registrationListener.register(server);
      }
    } catch (KeeperException non) {
      LOGGER.info("No node exists!!");
    }
    createServerNodes();
    createNotificationNode();
    if (registrationListener != null) {
      LOGGER.info(
          "Nodes Manager Started successfully.. NOW ,There are currently {} " + "servers: {}",
          registrationListener.getRegisteredServers().size(),
          registrationListener.getRegisteredServers().toString());
    }
    getSignal.await();
  }

  public void cleanUpZkNodeRecursively() throws Exception, InterruptedException {
    CountDownLatch signal = new CountDownLatch(1);
    ZkUtils.deleteRecursive(PathUtil.SEPARATOR_CHAR + pathMap.get(PathEnum.NAMESPACE.name()),
        signal);
    signal.await();
  }

  /**
   * Bootstrap
   * 
   * @throws IOException
   * 
   * @throws Exception
   */
  public void defaultNodesOnStart() throws IOException {
    createServersMap();
    createPersistentNode(
        PathUtil.SEPARATOR_CHAR + zkConfiguration.getPathMap().get(PathEnum.NAMESPACE.name()),
        null);
    createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()), null);
    createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()), null);
    createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.CONFIGPATH.name()), null);
    createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.FILESYSTEM.name()), null);
    createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.SHARDING_TABLE.name()), null);
  }

  /**
   * Create the node on zookeeper Second parameter is optional and once needed, it required to be
   * passed as array index 0
   * 
   * @param node
   * @param data
   * @throws IOException
   */
  public void createPersistentNode(final String node, CountDownLatch signal, Object... data)
      throws IOException {
    createNode(node, signal, CreateMode.PERSISTENT, data);
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
   */
  public void saveObjectZkNode(String parentNode, CountDownLatch signal, Object object)
      throws IOException, IllegalArgumentException, IllegalAccessException, InterruptedException {
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
      createPersistentNodeSync(leafNodePath);
    }
    counter.await();
    signal.countDown();
  }

  public void createPersistentNodeSync(final String node) throws IOException, InterruptedException {
    CountDownLatch counter = new CountDownLatch(1);
    createNode(node, counter, CreateMode.PERSISTENT);
    counter.await();
  }

  public void createPersistentNodeASync(final String node) {
    Object ctx = null;
    zk.create(node, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createNodeCallBack,
        ctx);

  }

  StringCallback createNodeCallBack = new StringCallback() {

    @Override
    public void processResult(int rc, String node, Object ctx, String name) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          try {
            createNode(node, true);
          } catch (KeeperException | InterruptedException e) {

          }
          break;
        case OK:
          LOGGER.info("created node");
          break;
        case NODEEXISTS:
          LOGGER.info("Node Already Exists");
          break;
        case NONODE:
          try {
            createNode(node, true);
          } catch (KeeperException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          break;
        default:
          /*
           * LOGGER.error("Unexpected result while trying to create node " + node + ": " +
           * KeeperException.create(KeeperException.Code.get(rc), node));
           */

      }

    }

    private void createNode(final String node, boolean makeLastNode)
        throws KeeperException, InterruptedException {
      PathUtils.validatePath(node);

      int pos = 1; // skip first slash, root is guaranteed to exist
      do {
        pos = node.indexOf('/', pos + 1);

        if (pos == -1) {
          if (makeLastNode) {
            pos = node.length();
          } else {
            break;
          }
        }

        String nodePath = node.substring(0, pos);
        if (zk.exists(nodePath, false) == null) {

          createPersistentNodeASync(nodePath);

        }

      } while (pos < node.length());
    }

  };

  public int getQueuedEvents() {
    int queuedEvents = 0;
    if (zk != null) {
      String zkString = zk.toString();
      zkString = zkString.substring(zkString.lastIndexOf("queuedevents"));
      queuedEvents = Integer.valueOf(zkString.split(":")[1]);
    }

    return queuedEvents;

  }

  /**
   * To Create the singal node as a name of the zookeeper node.
   * 
   * @param node name of the zookeeper node.
   * @param signal
   * @param isTransient
   * @throws IOException
   */
  public void createPersistentNode(final String node, CountDownLatch signal, boolean isTransient)
      throws IOException {
    if (isTransient) {
      if (signal != null) {
        signal.countDown();
      }
      return;
    }
    createNode(node, signal, CreateMode.PERSISTENT);
  }

  private void createNode(final String node, CountDownLatch signal, CreateMode createMode,
      Object... data) throws IOException {
    zk.create(node, (data != null && data.length != 0) ? ZkUtils.serialize(data[0]) : null,
        ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, new AsyncCallback.StringCallback() {
          @Override
          public void processResult(int rc, String path, Object ctx, String name) {
            try {
              switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                  retryCreationOfNode(signal, createMode, path, data);
                  break;
                case OK:
                  LOGGER.info("Server Monitoring Path [" + path + "] is created");
                  if (path.contains(CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name())
                      && path.contains("_job")) {
                    LOGGER.info("DELETE THE PULL/PUSH NODES");
                    deleteNode(path);
                    String oldString = CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name();
                    String newString = CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name();
                    deleteNode(path.replace(oldString, newString));
                  }
                  countDown(signal);
                  break;
                case NODEEXISTS:
                  LOGGER.warn("Server Monitoring Path [" + path + "] already exists");
                  countDown(signal);
                  break;
                case NONODE:
                  createParentNode(node);
                  retryCreationOfNode(signal, createMode, path, data);
                  break;
                default:
                  LOGGER.error("Unexpected result while trying to create node " + node + ": "
                      + KeeperException.create(KeeperException.Code.get(rc), path));
                  countDown(signal);
              }
            } catch (KeeperException | InterruptedException exception) {
              LOGGER.error("Unexpected result while trying to create node {} ", node);
              throw new RuntimeException(exception);
            }
          }

          private void createParentNode(final String node)
              throws InterruptedException, KeeperException {
            String parent = node.substring(0, node.lastIndexOf("/"));
            if (StringUtils.isNotBlank(parent)) {
              ZKPaths.mkdirs(zk, parent);
            }
          }

          private void retryCreationOfNode(CountDownLatch signal, CreateMode createMode,
              String path, Object... data) {
            try {
              createNode(path, signal, createMode, data);
            } catch (IOException e) {
              LOGGER.warn("Unable to redirect to create node");
            }
          }

          private void countDown(CountDownLatch signal) {
            if (getSignal != null) {
              getSignal.countDown();
            }
            if (signal != null) {
              signal.countDown();
            }
          }
        }, null);
  }

  public void createEphemeralNode(final String node, CountDownLatch signal, Object... data)
      throws IOException {
    createNode(node, signal, CreateMode.EPHEMERAL, data);
  }

  /**
   * @throws InterruptedException
   */
  public void shutdown() throws InterruptedException {
    if (zk != null) {
      zk.close();
    }
  }

  @Override
  public synchronized void process(WatchedEvent watchedEvent) {
    if (!watchedEvent.getType().name().equalsIgnoreCase("none")) {
      LOGGER.info("NodeManager watched event [{}] for {}", watchedEvent.getType(),
          watchedEvent.getPath());
    }
    switch (watchedEvent.getType()) {
      case None:
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
          LOGGER.info("ZooKeeper client connected to server");
        }
        connectedSignal.countDown();
        break;
      case NodeChildrenChanged:
        try {
          List<String> servers =
              zk.getChildren(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()), this);
          Collections.sort(servers);
          LOGGER.info("Server modified. Watched servers NOW: {}", servers.toString());
          Map<String, Set<Server>> diffs = computeDiff(servers);
          LOGGER.info("Removed Servers: {}", diffs.get(REMOVED));
          LOGGER.info("Added servers: {}", diffs.get(ADDED));
          processDiffs(diffs);

        } catch (KeeperException | InterruptedException e) {
          LOGGER.info("There was an error retrieving the list of " + "servers [{}]",
              e.getLocalizedMessage(), e);
        }
        break;
      case NodeDataChanged:
        LOGGER.info("A node changed: {} {}", watchedEvent.getPath(), watchedEvent.getState());
        try {
          Server server = getServerInfo(watchedEvent.getPath());
          registrationListener.updateServer(server);
          zk.exists(watchedEvent.getPath(), this);
        } catch (KeeperException | InterruptedException ex) {
          LOGGER.info("There was an error while updating the data " + "associated with node {}: {}",
              watchedEvent.getPath(), ex.getLocalizedMessage());
        }
        break;
      case NodeDeleted:
        LOGGER.info("A node is deleted: {} ", watchedEvent.getPath());
        break;
      case NodeCreated:
        LOGGER.info(" A node is created: {}", watchedEvent.getPath());
        break;
      default:
        LOGGER.info("Not an expected event: {}; for {}", watchedEvent.getType(),
            watchedEvent.getPath());
    }
  }

  /**
   * To process the difference of the server for added and removed
   * 
   * @param diffs
   */
  private synchronized void processDiffs(Map<String, Set<Server>> diffs) {
    for (Server server : diffs.get(REMOVED)) {
      LOGGER.info("Reporting eviction to listener: " + server.getServerAddress().getHostname());
      evictionListener.deregister(server);
      registerAlert(server, false);
    }
    for (Server server : diffs.get(ADDED)) {
      LOGGER.info("Reporting addition to listener: " + server.getName());
      registrationListener.register(server);
      removeAlert(server);
    }

  }

  /**
   * To get monitored server
   * 
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public List<String> getMonitoredServers() throws KeeperException, InterruptedException {
    List<String> serverList =
        zk.getChildren(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()), this);
    Collections.sort(serverList);
    return serverList;
  }

  public List<String> getChildren(String parentNode) throws KeeperException, InterruptedException {
    System.out.println(parentNode);
    List<String> children = zk.getChildren(parentNode, this);
    return children;
  }

  /**
   * To compute difference
   * 
   * @param latest
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public synchronized Map<String, Set<Server>> computeDiff(List<String> latest)
      throws KeeperException, InterruptedException {
    Set<Server> knownServers = registrationListener.getRegisteredServers();
    Set<String> latestServersNames = Sets.newHashSet(latest);
    Set<Server> evictedServers = Sets.newHashSet();
    for (Server server : knownServers) {
      if (!latestServersNames.remove(server.getName())) {
        LOGGER.info("Server " + server.getName() + " has been removed");
        evictedServers.add(server);
      }
    }
    Set<Server> newlyAddedServers = Sets.newHashSet();
    for (String name : latestServersNames) {
      LOGGER.info("Server " + name + " has joined the monitored pool");
      newlyAddedServers.add(getServerInfo(name));
    }
    Map<String, Set<Server>> diffs = Maps.newHashMapWithExpectedSize(2);
    diffs.put(ADDED, newlyAddedServers);
    diffs.put(REMOVED, evictedServers);
    return diffs;
  }

  /**
   * Get server information
   * 
   * @param name
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Server getServerInfo(String name) throws KeeperException, InterruptedException {
    String fullPath = buildMonitorPathForServer(name);
    Stat stat = new Stat();
    byte[] data = zk.getData(fullPath, this, stat);
    if (data == null || data.length == 0) {
      return null;
    }
    Server server = serverNameMap.get(name);
    if (server == null) {
      return null;
    }
    LOGGER.info("Server: {} -- Payload: {}", server.getName(), server.getData());
    return server;
  }

  /**
   * @param server
   * @param persistent
   * @return
   */
  synchronized public Status registerAlert(Server server, boolean persistent) {
    String path = buildAlertPathForServer(server);
    try {
      Stat stat = zk.exists(path, false);
      if (stat == null) {
        CreateMode createMode = persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
        zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode,
            ZkUtils.checkCreateAlertStatusAsync(), server);
      }
    } catch (KeeperException.SessionExpiredException ex) {

      throw new RuntimeException("ZK Session Expired, probably caused by a connectivity "
          + "loss or, more worryingly, because we've lost all connectivity to the ZK "
          + "ensemble.");
    } catch (KeeperException | InterruptedException e) {
      String msg = String.format("Caught an exception while trying to alert {} ({})", server,
          e.getLocalizedMessage());
      LOGGER.info("Caught an exception while trying to alert {} ({})", server,
          e.getLocalizedMessage());
      return Status.createErrorStatus(msg);
    }
    return Status.createStatus("Server " + server.getName() + " alerted");
  }

  /**
   * Removes the alert for the server, for example, when the server re-starts after an unexpected
   * termination (that triggered an alert and a subsequent 'alert' to be set).
   * 
   * @param server
   */
  synchronized public void removeAlert(Server server) {
    String path = buildAlertPathForServer(server);
    try {
      Stat stat = zk.exists(path, this);
      if (stat != null) {
        zk.delete(path, stat.getVersion(), ZkUtils.checkAlertRemoveStatusAsync(), server);
      }
      path = buildMonitorPathForServer(server);
      if (zk.exists(path, this) != null) {
        LOGGER.info("Removed alert for node: " + server.getName());
      }
    } catch (KeeperException | InterruptedException kex) {
      LOGGER.info(
          "Exception encountered ('{}') while pruning the {} "
              + "branch, upon registration of {} - this is probably safe to ignore, "
              + "unless other unexplained errors start to crop up",
          new Object[] {kex.getLocalizedMessage(), path, server.getName()});
    }
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
    return zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()) + PathUtil.SEPARATOR_CHAR
        + serverHostname;
  }

  public String buildAlertPathByName(String nodeName) {
    return zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()) + PathUtil.SEPARATOR_CHAR
        + nodeName;
  }

  /**
   * Build path for Server
   * 
   * @param server
   * @return String
   */
  public String buildMonitorPathForServer(Server server) {
    return buildMonitorPathForServer(server.getServerAddress().getHostname());
  }

  /**
   * @param serverHostname
   * @return
   */
  protected String buildMonitorPathForServer(String serverHostname) {
    if (serverHostname.startsWith(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()))) {
      return serverHostname;
    } else if (serverHostname
        .startsWith(zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()))) {
      serverHostname = serverHostname.substring(serverHostname.lastIndexOf(File.separatorChar) + 1);
    }
    return zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()) + PathUtil.SEPARATOR_CHAR
        + serverHostname;
  }

  public String buildConfigPath(String fileName) {
    String buildPath = zkConfiguration.getPathMap().get(PathEnum.CONFIGPATH.name())
        + PathUtil.SEPARATOR_CHAR + fileName;
    return buildPath;
  }

  public String buildPathNode(String parentNode)
      throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
    Set<LeafBean> beans = ZkUtils.searchLeafNode(parentNode, null, null);
    String path = "";
    for (LeafBean bean : beans) {
      if (bean.getName().equalsIgnoreCase(parentNode)) {
        path = bean.getPath() + PathUtil.SEPARATOR_CHAR + parentNode + PathUtil.SEPARATOR_CHAR;
        break;
      } else if (bean.getPath().contains(String.valueOf(PathUtil.SEPARATOR_CHAR))) {
        path = bean.getPath() + PathUtil.SEPARATOR_CHAR;
      }
    }
    return path;
  }

  public byte[] getNodeData(String pathNode) throws KeeperException, InterruptedException {
    Stat stat = zk.exists(pathNode, this);
    if (stat != null) {
      return zk.getData(pathNode, false, stat);
    }
    return null;
  }

  /**
   * Create the servers map which all need to be run on nodes
   * 
   */
  private void createServersMap() {
    List<String> checkUnique = new ArrayList<String>();
    List<Node> nodes;
    nodes = CoordinationApplicationContext.getLocalClusterConfig().getNode();

    for (int index = 0; index < nodes.size(); index++) {
      Node node = nodes.get(index);
      if (!checkUnique.contains(node.getIp())) {
        Server server = new Server();
        server.setServerAddress(new ServerAddress(NODE_NAME_PRIFIX + index, node.getIp()));
        server.setData(new Date().getTime());
        server.setServerType("simpleserver");
        server.setCurrentDateTime(ZkUtils.getCurrentTimeStamp());
        server.setDescription("A simple server to test monitoring");
        server.setId(index);
        this.servers.add(server);
        checkUnique.add(node.getIp());
        serverNameMap.put(server.getServerAddress().getHostname(), server);
        LOGGER.info("SERVER NAME AND IP :: [" + server.getName() + " , "
            + server.getServerAddress().getIp() + "]");
      }
      getSignal = new CountDownLatch(this.servers.size() + DEFAULT_NODES);
    }
  }

  /**
   * Create nodes on zookeeper server for each servers/nodes of application
   * 
   * @throws IOException
   */
  private void createServerNodes() throws IOException {
    for (Server server : this.servers) {
      String nodePath = buildMonitorPathForServer(server);
      createPersistentNode(nodePath, null);
    }

  }

  /**
   * Delete particular node/server
   * 
   * @param server
   */
  public synchronized void deleteNode(Server server) {
    String path = buildMonitorPathForServer(server);
    Stat stat = null;
    try {
      stat = zk.exists(path, this);
      if (stat != null) {
        ZkUtils.deleteRecursive(path, null);
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.info(
          "Unable to delete node :: " + server.getName() + " Exception is :: " + e.getMessage());
      LOGGER.info(" PLEASE CHECK, ZOOKEEPER SERVER IS RUNNING or NOT!!");
    } catch (Exception e) {
      LOGGER.info("Unable to delete the node");
    }

  }

  public synchronized void checkZookeeperConnection(Server server) {
    String path = buildMonitorPathForServer(server);
    zk.exists(path, this, ZkUtils.checkZKConnectionStatusAsync(), server);
  }

  /**
   * Save the file on ZNode of zookeeper
   * 
   * @param file
   * @throws IOException
   */
  public void saveConfigFileToZNode(ZKNodeFile file, CountDownLatch signal) throws IOException {
    createPersistentNode(buildConfigPath(file.getFileName()), signal, file);
  }

  public void saveShardingConfigFileToZNode(String path, ZKNodeFile file, CountDownLatch signal)
      throws IOException {
    String filePath = path + File.separatorChar + file.getFileName();
    createPersistentNode(filePath, signal, file);
  }

  /**
   * This will return the object which will later be type casted to ConfigFile
   * 
   * @param fileName
   * @return object
   * @throws KeeperException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public Object getConfigFileFromZNode(String fileName)
      throws KeeperException, InterruptedException, ClassNotFoundException, IOException {
    Stat stat = null;
    stat = zk.exists(buildConfigPath(fileName), this);
    if (stat != null) {
      return ZkUtils.deserialize(zk.getData(buildConfigPath(fileName), this, stat));
    }
    return null;
  }

  public Object getObjectFromZKNode(String nodePath)
      throws KeeperException, InterruptedException, ClassNotFoundException, IOException {
    Stat stat = null;
    stat = zk.exists(nodePath, this);
    if (stat != null) {
      return ZkUtils.deserialize(zk.getData(nodePath, this, stat));
    }
    return null;
  }

  public String getStringFromZKNode(String nodePath)
      throws KeeperException, InterruptedException, ClassNotFoundException, IOException {
    Stat stat = null;
    stat = zk.exists(nodePath, this);
    if (stat != null) {
      return new String(zk.getData(nodePath, this, stat), Charsets.UTF_8);
    }
    return null;
  }

  public synchronized void deleteNode(String nodePath) {
    Stat stat = null;
    try {
      stat = zk.exists(nodePath, this);
      if (stat != null) {
        zk.delete(nodePath, stat.getVersion(), ZkUtils.checkDeleteNodeStatusAsync(), nodePath);
        LOGGER.info("Node {} is deleted successfully", nodePath);
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.info("Unable to delete node :: " + nodePath + " Exception is :: " + e.getMessage());
      LOGGER.info(" PLEASE CHECK, ZOOKEEPER SERVER IS RUNNING or NOT!!");
    }

  }

  private boolean createNotificationNode() {
    boolean flag = false;
    try {
      for (int nodeId = 0; nodeId < this.servers.size(); nodeId++) {
        String pushNotification = pathMap.get(PathEnum.BASEPATH.name()) + PathUtil.SEPARATOR_CHAR
            + (NODE_NAME_PRIFIX + nodeId) + PathUtil.SEPARATOR_CHAR
            + CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name();
        createPersistentNode(pushNotification, null);
      }
      flag = true;
    } catch (IOException ex) {
      flag = false;
    }
    return flag;
  }

  public void setObjectToZKNode(String nodePath, Object data)
      throws KeeperException, InterruptedException, ClassNotFoundException, IOException {
    Stat stat = null;
    stat = zk.exists(nodePath, this);
    if (stat != null) {
      zk.setData(nodePath, ZkUtils.serialize(data), stat.getVersion());
    }
  }

  public void setObjectToZKNode(String nodePath, byte[] data)
      throws KeeperException, InterruptedException, ClassNotFoundException, IOException {
    Stat stat = null;
    stat = zk.exists(nodePath, this);
    if (stat != null) {
      zk.setData(nodePath, data, stat.getVersion());
    }
  }

  /**
   * Checks if a nodePath exists in zookeeper
   * 
   * @param nodePath
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean checkNodeExists(String nodePath) throws KeeperException, InterruptedException {
    Stat stat = zk.exists(nodePath, this);
    return stat != null;
  }
}
