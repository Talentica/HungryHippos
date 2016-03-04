/**
 * 
 */
package com.talentica.hungryHippos.coordination;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.ServerAddress;
import com.talentica.hungryHippos.coordination.domain.Status;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.domain.ZookeeperConfiguration;
import com.talentica.hungryHippos.coordination.listeners.AlertManager;
import com.talentica.hungryHippos.coordination.listeners.EvictionListener;
import com.talentica.hungryHippos.coordination.listeners.RegistrationListener;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathEnum;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;

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
    private Map<String,Server> serverNameMap;
    private static final Logger LOGGER = LoggerFactory.getLogger(NodesManager.class.getName());
    private static final int DEFAULT_NODES = PathEnum.values().length-1;// subtracting -1 because HOSTPATH is separately created. Refer bootstrap() method. 
    private Map<String,String> pathMap;
    private static final String NODE_NAME_PRIFIX = "_node"; 
    private String formatFlag;
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
		pathMap = new HashMap<String,String>();
		pathMap.put(PathEnum.NAMESPACE.name(), Property.getPropertyValue("zookeeper.namespace_path"));
		pathMap.put(PathEnum.BASEPATH.name(), Property.getPropertyValue("zookeeper.base_path"));
		pathMap.put(PathEnum.ZKIPTPATH.name(), Property.getPropertyValue("zookeeper.server.ips"));
		pathMap.put(PathEnum.ALERTPATH.name(), Property.getPropertyValue("zookeeper.alerts_path"));
		pathMap.put(PathEnum.CONFIGPATH.name(), Property.getPropertyValue("zookeeper.config_path"));
		Integer sessionTimeOut = Integer.valueOf(Property.getPropertyValue("zookeeper.session_timeout"));
		zkConfiguration = new ZookeeperConfiguration(pathMap,sessionTimeOut);
		connectZookeeper();
	}

	public void connectZookeeper() {
		LOGGER.info("Node Manager started, connecting to ZK hosts: "
				+ zkConfiguration.getPathMap().get(PathEnum.ZKIPTPATH.name()));
		try {
			connect(zkConfiguration.getPathMap().get(PathEnum.ZKIPTPATH.name()));
			LOGGER.info("Connected - Session ID: " + zk.getSessionId());
		} catch (Exception e) {
			LOGGER.info("Could not connect to Zookeper instance" + e);
		}
	}
	 
	/**
	 * To connect the with zookeeper host
	 * 
	 * @param hosts
	 * @throws Exception 
	 */
	private void connect(String hosts) throws Exception {
		zk = new org.apache.zookeeper.ZooKeeper(hosts,
				zkConfiguration.getSessionTimeout(), this);
		ZKUtils.zk = zk;
		ZKUtils.nodesManager = this;
		connectedSignal.await();
	}
	 

	    /**
	     * To start the application
	     * 
	     * @throws Exception
	     */
	public void startup() throws Exception {
	    	formatFlag = Property.getPropertyValue("cleanup.zookeeper.nodes").toString();
	    	if(formatFlag.equals("Y")){
	    		CountDownLatch signal = new CountDownLatch(1);
	    		ZKUtils.deleteRecursive(PathUtil.FORWARD_SLASH + pathMap.get(PathEnum.NAMESPACE.name()),signal);
	    		signal.await();
	    	}
	    	
	        defaultNodesOnStart();
		try {
			List<String> serverNames = getMonitoredServers();			
			LOGGER.info("Starting Node Manager.. Already the server/servers {} running!",serverNames);
			for (String name : serverNames) {
				Server server = getServerInfo(name);
				if (server != null)
					registrationListener.register(server);
			}
		}catch(KeeperException non){
	        	LOGGER.info("No node exists!!");
	        }
		 createServerNodes();
		 createNotificationNode();
		 if(registrationListener != null){
	        LOGGER.info("Nodes Manager Started successfully.. NOW ,There are currently {} " +
	                "servers: {}", registrationListener.getRegisteredServers().size(), registrationListener.getRegisteredServers().toString());
		 }
		 getSignal.await();
	    }

	    /**
	     * Bootstrap
	     * @throws IOException 
	     * 
	     * @throws Exception
	     */
	public void defaultNodesOnStart() throws IOException{
		createServersMap();
		createPersistentNode(PathUtil.FORWARD_SLASH+zkConfiguration.getPathMap().get(PathEnum.NAMESPACE.name()),null);
		createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()),null);
		createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()),null);
		createPersistentNode(zkConfiguration.getPathMap().get(PathEnum.CONFIGPATH.name()),null);
	}
	    /**
	     * Create the node on zookeeper
	     * Second parameter is optional and once needed, it required to be passed as array index 0
	     * @param node
	     * @param data
	     * @throws IOException 
	     */
	    public void createPersistentNode(final String node,CountDownLatch signal ,Object ...data) throws IOException {
		createNode(node, signal, CreateMode.PERSISTENT, data);
	    }

	private void createNode(final String node, CountDownLatch signal, CreateMode createMode, Object... data)
			throws IOException {
		zk.create(node, (data != null && data.length != 0) ? ZKUtils.serialize(data[0]) : node.getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, new AsyncCallback.StringCallback() {
					@Override
					public void processResult(int rc, String path, Object ctx, String name) {
						switch (KeeperException.Code.get(rc)) {
						case CONNECTIONLOSS:
							try {
								createNode(path, signal,createMode, data);
							} catch (IOException e) {
								LOGGER.warn("Unable to redirect to create node");
							}
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
							if (signal != null)
								signal.countDown();
							if (getSignal != null)
								getSignal.countDown();
							break;
						case NODEEXISTS:
							LOGGER.info("Server Monitoring Path [" + path + "] already exists");
							if (getSignal != null)
								getSignal.countDown();
							break;
						default:
							LOGGER.info("Unexpected result while trying to create node " + node + ": "
									+ KeeperException.create(KeeperException.Code.get(rc), path));
						}
					}
				}, null);
	}

	public void createEphemeralNode(final String node, CountDownLatch signal, Object... data) throws IOException {
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
			LOGGER.info(
					"NodeManager watched event [{}] for {}",
					watchedEvent.getType(), watchedEvent.getPath());
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
	                		List<String> servers = zk.getChildren(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()), this);
	                		Collections.sort(servers);
		                    LOGGER.info("Server modified. Watched servers NOW: {}",
		                            servers.toString());
		                    Map<String, Set<Server>> diffs = computeDiff(servers);
		                    LOGGER.info("Removed Servers: {}", diffs.get(REMOVED));
		                    LOGGER.info("Added servers: {}", diffs.get(ADDED));
		                    	 processDiffs(diffs);
	                    
	                } catch (KeeperException | InterruptedException e) {
	                	LOGGER.info("There was an error retrieving the list of " +
	                            "servers [{}]", e.getLocalizedMessage(), e);
	                }
	                break;
	            case NodeDataChanged:
	            	LOGGER.info("A node changed: {} {}", watchedEvent.getPath(),
	                        watchedEvent.getState());
	                try {
	                    Server server = getServerInfo(watchedEvent.getPath());
	                    registrationListener.updateServer(server);
	                    zk.exists(watchedEvent.getPath(), this);
	                } catch (KeeperException | InterruptedException ex) {
	                	LOGGER.info("There was an error while updating the data " +
	                            "associated with node {}: {}", watchedEvent.getPath(),
	                            ex.getLocalizedMessage());
	                }
	                break;
	            case NodeDeleted:
	            	LOGGER.info("A node is deleted: {} ",watchedEvent.getPath());
	            	break;
	            case NodeCreated:
	            	LOGGER.info(" A node is created: {}",watchedEvent.getPath());
	            	break;
	            default:
	            	LOGGER.info("Not an expected event: {}; for {}",
	                        watchedEvent.getType(), watchedEvent.getPath());
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
	    	List<String> serverList = zk.getChildren(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()), this);
	    	Collections.sort(serverList);
	        return serverList;
	    }

	    public List<String> getChildren(String parentNode) throws KeeperException, InterruptedException{
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
	        if(data==null || data.length == 0){
				return null;
			}
			Server server = serverNameMap.get(name);
			if(server == null){
				return null;
			}
			LOGGER.info("Server: {} -- Payload: {}", server.getName(),
			        server.getData());
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
	                        ZKUtils.checkCreateAlertStatusAsync(), server);
	            }
	        } catch (KeeperException.SessionExpiredException ex) {
	           
	            throw new RuntimeException("ZK Session Expired, probably caused by a connectivity " +
	                    "loss or, more worryingly, because we've lost all connectivity to the ZK " +
	                    "ensemble.");
	        } catch (KeeperException | InterruptedException e) {
	            String msg = String.format("Caught an exception while trying to alert {} ({})",
	                    server, e.getLocalizedMessage());
	            LOGGER.info("Caught an exception while trying to alert {} ({})",
	                    server, e.getLocalizedMessage());
	            return Status.createErrorStatus(msg);
	        }
	        return Status.createStatus("Server " + server.getName() + " alerted");
	    }

	    /**
	     * Removes the alert for the server, for example, when the server re-starts after an
	     * unexpected termination (that triggered an alert and a subsequent 'alert' to be set).
	     * @param server
	     */
	    synchronized public void removeAlert(Server server) {
	        String path = buildAlertPathForServer(server);
	        try {
	            Stat stat = zk.exists(path, this);
	            if (stat != null) {
	                zk.delete(path, stat.getVersion(), ZKUtils.checkAlertRemoveStatusAsync(), server);	               
	            }
	            path = buildMonitorPathForServer(server);
	            if (zk.exists(path, this) != null) {
	            	LOGGER.info("Removed alert for node: " + server.getName());
	            }
	        } catch (KeeperException | InterruptedException kex) {
	        	LOGGER.info("Exception encountered ('{}') while pruning the {} " +
	                    "branch, upon registration of {} - this is probably safe to ignore, " +
	                    "unless other unexplained errors start to crop up",
	                    new Object[]{kex.getLocalizedMessage(), path, server.getName()});
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
	    	return zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()) + PathUtil.FORWARD_SLASH + serverHostname;
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
		} else if (serverHostname.startsWith(zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()))) {
			serverHostname = serverHostname.substring(serverHostname.lastIndexOf(File.separatorChar) + 1);
		}
		return zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()) + PathUtil.FORWARD_SLASH + serverHostname;
		}
	    
	    public String buildConfigPath(String fileName){
	    	String buildPath = zkConfiguration.getPathMap().get(PathEnum.CONFIGPATH.name()) + PathUtil.FORWARD_SLASH + fileName;
	    	return buildPath;
	    }
	    
	public String buildPathNode(String parentNode) throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
		Set<LeafBean> beans = ZKUtils.searchTree(parentNode, null,null);
		String path = "";
		for(LeafBean bean : beans){
			if(bean.getName().equalsIgnoreCase(parentNode)){
				path = bean.getPath() + PathUtil.FORWARD_SLASH+ parentNode + PathUtil.FORWARD_SLASH;
				break;
			}else if(bean.getPath().contains(PathUtil.FORWARD_SLASH)){
				path = bean.getPath() + PathUtil.FORWARD_SLASH;
			}
		}
		return path;
	}
	
	public byte[] getNodeData(String pathNode) throws KeeperException, InterruptedException{
		 Stat stat = zk.exists(pathNode, this);
		 if(stat != null){
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
			Properties property = Property.loadServerProperties();
			int nodeIndex = 0;
			for(Object key : property.keySet()){
			String srv = property.getProperty((String)key);
			String IP = null;
			String PORT = null;
			
				IP = srv.split(":")[0];
				PORT = srv.split(":")[1];
				
			if (!checkUnique.contains(IP)) {
				Server server = new Server();
				server.setServerAddress(new ServerAddress(NODE_NAME_PRIFIX + nodeIndex, IP));
				server.setPort(Integer.valueOf(PORT));
				server.setData(new Date().getTime());
				server.setServerType("simpleserver");
				server.setCurrentDateTime(ZKUtils.getCurrentTimeStamp());
				server.setDescription("A simple server to test monitoring");
				server.setId(nodeIndex);
				this.servers.add(server);
				checkUnique.add(IP);
				serverNameMap.put(server.getServerAddress().getHostname(), server);
				nodeIndex++;
				LOGGER.info("\tSERVER NAME AND IP :: ["+server.getName()+" , "+server.getServerAddress().getIp()+"]");				
				}
			 getSignal = new CountDownLatch(this.servers.size()+DEFAULT_NODES);// additional 3 is added because of namesapce,alert and basepath node is also created
			}
		}
		
		/**
		 * Create nodes on zookeeper server for each servers/nodes of application
		 * 
		 * @throws IOException
		 */
		private void createServerNodes() throws IOException{			
			for(Server server : this.servers){
				String nodePath = buildMonitorPathForServer(server);
				createPersistentNode(nodePath,null);
			}
			
		}
		
		/**
		 * Delete particular node/server
		 * 
		 * @param server
		 */
		public synchronized void deleteNode(Server server){
			 String path = buildMonitorPathForServer(server);
			 Stat stat = null;
			try {
				stat = zk.exists(path, this);
				if(stat != null){
					ZKUtils.deleteRecursive(path, null);
				}
			} catch (KeeperException | InterruptedException e) {
				LOGGER.info("Unable to delete node :: " + server.getName() + " Exception is :: "+ e.getMessage());
				LOGGER.info(" PLEASE CHECK, ZOOKEEPER SERVER IS RUNNING or NOT!!");
			} catch (Exception e) {
				LOGGER.info("Unable to delete the node");
			}
			
		}
		
		public synchronized void checkZookeeperConnection(Server server){
			String path = buildMonitorPathForServer(server);
			zk.exists(path, this,ZKUtils.checkZKConnectionStatusAsync(),server);
		}
		
	/**
	 * Save the file on ZNode of zookeeper
	 * 
	 * @param file
	 * @throws IOException
	 */
	public void saveConfigFileToZNode(ZKNodeFile file , CountDownLatch signal) throws IOException {
		createPersistentNode(buildConfigPath(file.getFileName()),signal,
				file);
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
	public Object getConfigFileFromZNode(String fileName) throws KeeperException, InterruptedException, ClassNotFoundException, IOException{
		 Stat stat = null;
				stat = zk.exists(buildConfigPath(fileName), this);
				if(stat != null){
					return ZKUtils.deserialize(zk.getData(buildConfigPath(fileName), this,stat));
				 }
		return null;
	}
	
	public Object getObjectFromZKNode(String nodePath) throws KeeperException, InterruptedException, ClassNotFoundException, IOException{
		Stat stat = null;
		stat = zk.exists(nodePath, this);
		if(stat != null){
			return ZKUtils.deserialize(zk.getData(nodePath, this,stat));
		 }
		return null;
	}
	
	public synchronized void deleteNode(String nodePath){
		 Stat stat = null;
		try {
			stat = zk.exists(nodePath, this);
			if(stat != null){
			 zk.delete(nodePath, stat.getVersion(), ZKUtils.checkDeleteNodeStatusAsync(), nodePath);
			 LOGGER.info("Node {} is deleted successfully",nodePath);
			}
		} catch (KeeperException | InterruptedException e) {
			LOGGER.info("Unable to delete node :: " + nodePath + " Exception is :: "+ e.getMessage());
			LOGGER.info(" PLEASE CHECK, ZOOKEEPER SERVER IS RUNNING or NOT!!");
		}
		
	}
	
	private boolean createNotificationNode() {
		boolean flag = false;
		try {
			for (int nodeId = 0; nodeId < this.servers.size(); nodeId++) {
				String pushNotification = pathMap.get(PathEnum.BASEPATH.name())
						+ PathUtil.FORWARD_SLASH
						+ (NODE_NAME_PRIFIX + nodeId)
						+ PathUtil.FORWARD_SLASH
						+ CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name();
				createPersistentNode(pushNotification,null);
			}
			flag = true;
		} catch (IOException ex) {
			flag = false;
		}
		return flag;
	}
	
}
