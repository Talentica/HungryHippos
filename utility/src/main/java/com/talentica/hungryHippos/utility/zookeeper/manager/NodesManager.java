/**
 * 
 */
package com.talentica.hungryHippos.utility.zookeeper.manager;

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
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.talentica.hungryHippos.utility.PathEnum;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.zookeeper.AlertManager;
import com.talentica.hungryHippos.utility.zookeeper.EvictionListener;
import com.talentica.hungryHippos.utility.zookeeper.RegistrationListener;
import com.talentica.hungryHippos.utility.zookeeper.Server;
import com.talentica.hungryHippos.utility.zookeeper.ServerAddress;
import com.talentica.hungryHippos.utility.zookeeper.Status;
import com.talentica.hungryHippos.utility.zookeeper.ZKNodeFile;
import com.talentica.hungryHippos.utility.zookeeper.ZKUtils;
import com.talentica.hungryHippos.utility.zookeeper.ZookeeperConfiguration;

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
    private Properties prop;
    private static final Logger LOGGER = LoggerFactory.getLogger(NodesManager.class.getName());
    private static final int DEFAULT_NODES = PathEnum.values().length-1;// subtracting -1 because HOSTPATH is separately created. Refer bootstrap() method. 
    private Map<String,String> pathMap;
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
		pathMap = new HashMap<String,String>();
		prop = Property.getProperties();
		pathMap.put(PathEnum.NAMESPACE.name(),prop.getProperty("zookeeper.namespace_path"));
		pathMap.put(PathEnum.BASEPATH.name(),prop.getProperty("zookeeper.base_path"));
		pathMap.put(PathEnum.HOSTPATH.name(), prop.getProperty("zookeeper.server.ips"));
		pathMap.put(PathEnum.ALERTPATH.name(),prop.getProperty("zookeeper.alerts_path"));
		pathMap.put(PathEnum.CONFIGPATH.name(),prop.getProperty("zookeeper.config_path"));
		Integer sessionTimeOut = Integer.valueOf(prop
				.getProperty("zookeeper.session_timeout"));
		zkConfiguration = new ZookeeperConfiguration(pathMap,sessionTimeOut);
		connectZookeeper();
	}

	public void connectZookeeper() {
		LOGGER.info("Node Manager started, connecting to ZK hosts: "
				+ zkConfiguration.getPathMap().get(PathEnum.HOSTPATH.name()));
		try {
			connect(zkConfiguration.getPathMap().get(PathEnum.HOSTPATH.name()));
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
		connectedSignal.await();
	}
	 

	    /**
	     * To start the application
	     * 
	     * @throws Exception
	     */
	    public  void startup() throws Exception {
	        bootstrap();
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
		 if(registrationListener != null){
	        LOGGER.info("Nodes Manager Started successfully.. NOW ,There are currently %d " +
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
	public void bootstrap() throws IOException{
		createServersMap();
		createNode(PathUtil.FORWARD_SLASH+zkConfiguration.getPathMap().get(PathEnum.NAMESPACE.name()));
		createNode(zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()));
		createNode(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()));
		createNode(zkConfiguration.getPathMap().get(PathEnum.CONFIGPATH.name()));
	}
	    /**
	     * Create the node on zookeeper
	     * Second parameter is optional and once needed, it required to be passed as array index 0
	     * @param node
	     * @param data
	     * @throws IOException 
	     */
	    public void createNode(final String node,Object ...data) throws IOException {
	    	
	        zk.create(node, (data!=null && data.length != 0) ? ZKUtils.serialize(data[0]):node.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
	                new AsyncCallback.StringCallback() {
	                    @Override
	                    public void processResult(int rc, String path, Object ctx, String name) {
	                        switch (KeeperException.Code.get(rc)) {
	                            case CONNECTIONLOSS:
								try {
									createNode(path,data);
								} catch (IOException e) {
									LOGGER.warn("Unable to redirect to create node");
								}
	                                break;
	                            case OK:
	                                LOGGER.info("Server Monitoring Path [" + path + "] is created");
	                                getSignal.countDown();
	                                LOGGER.info(Thread.currentThread().getName());
	                                break;
	                            case NODEEXISTS:
	                            	LOGGER.info("Server Monitoring Path [" + path + "] already exists");
	                            	getSignal.countDown();
	                            	LOGGER.info(Thread.currentThread().getName());
	                                break;
	                            default:
	                            	LOGGER.info("Unexpected result while trying to create node " +
	                                        node + ": " + KeeperException.create(KeeperException.Code
	                                        .get(rc), path));
	                        }
	                    }
	                },
	                null    
	        );
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
	                    connectedSignal.countDown();
	                }
	                break;
	            case NodeChildrenChanged:
	                try {
	                	    LOGGER.info(Thread.currentThread().getName());
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
	            	LOGGER.info(Thread.currentThread().getName());
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
	            	LOGGER.info(Thread.currentThread().getName());
	            	LOGGER.info("A node is deleted: {} ",watchedEvent.getPath());
	            	break;
	            case NodeCreated:
	            	LOGGER.info(Thread.currentThread().getName());
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
		        	LOGGER.info("Reporting eviction to listener: " +
		                    server.getServerAddress().getHostname());
		        	evictionListener.deregister(server);
		            silence(server, false);
		        }
	        for (Server server : diffs.get(ADDED)) {
	        	LOGGER.info("Reporting addition to listener: " +
	                    server.getServerAddress().getHostname());
	            registrationListener.register(server);
	            removeSilence(server);
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
	        if(data.length == 0){
				return null;
			}
			//Server server = mapper.readValue(data, Server.class);
	        
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
	    synchronized public Status silence(Server server, boolean persistent) {
	        String path = buildAlertPathForServer(server);
	        try {
	            Stat stat = zk.exists(path, false);
	            if (stat == null) {
	                CreateMode createMode = persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
	                zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode,
	                        createSilenceCallback, server);
	            }
	        } catch (KeeperException.SessionExpiredException ex) {
	           
	            throw new RuntimeException("ZK Session Expired, probably caused by a connectivity " +
	                    "loss or, more worryingly, because we've lost all connectivity to the ZK " +
	                    "ensemble.");
	        } catch (KeeperException | InterruptedException e) {
	            String msg = String.format("Caught an exception while trying to silence {} ({})",
	                    server, e.getLocalizedMessage());
	            LOGGER.info("Caught an exception while trying to silence {} ({})",
	                    server, e.getLocalizedMessage());
	            return Status.createErrorStatus(msg);
	        }
	        return Status.createStatus("Server " + server.getName() + " silenced");
	    }

	    AsyncCallback.StringCallback createSilenceCallback = new AsyncCallback.StringCallback() {
	        @Override
	        public void processResult(int rc, String path, Object ctx, String name) {
	        	
	            Server svr = (Server) ctx;
	            switch (KeeperException.Code.get(rc)) {
	                case CONNECTIONLOSS:
	                    silence(svr, false);
	                    break;
	                case NODEEXISTS:
	                	LOGGER.info("Trying to silence an already silenced server [" + name + "]");
	                    break;
	                case OK:
	                	LOGGER.info("Server {} silenced ({})", svr.getName(),
	                            svr.getServerAddress().getHostname());	                   
	                    	try {
	                    		  if(getMonitoredServers() != null){
	                    			LOGGER.info("STATUS :: NOW, There are currently {} "
	                    						+ "servers: {}", getMonitoredServers().size(), getMonitoredServers().toString());	   
	                    		  }
	                    		} catch (Exception e) {
	                    			LOGGER.info("Unable to get the monitored servers");
	                    		}
	                    break;
	                default:
	                	LOGGER.info("[{}] Unexpected result for silencing {} ({})",
	                            new Object[]{KeeperException.Code.get(rc), svr.getName(), path});
	            }
	        }
	    };

	    /**
	     * Removes the silence for the server, for example, when the server re-starts after an
	     * unexpected termination (that triggered an alert and a subsequent 'silence' to be set).
	     * @param server
	     */
	    synchronized public void removeSilence(Server server) {
	        String path = buildAlertPathForServer(server);
	        try {
	            Stat stat = zk.exists(path, this);
	            if (stat != null) {
	                zk.delete(path, stat.getVersion(), removeSilenceCallback, server);	               
	            }
	            path = buildMonitorPathForServer(server);
	            if (zk.exists(path, this) != null) {
	            	LOGGER.info("Removed silence for node: " + server.getName());
	            }
	        } catch (KeeperException | InterruptedException kex) {
	        	LOGGER.info("Exception encountered ('{}') while pruning the {} " +
	                    "branch, upon registration of {} - this is probably safe to ignore, " +
	                    "unless other unexplained errors start to crop up",
	                    new Object[]{kex.getLocalizedMessage(), path, server.getName()});
	        }
	    }

	    AsyncCallback.VoidCallback removeSilenceCallback = new AsyncCallback.VoidCallback() {
	        @Override
	        public void processResult(int rc, String path, Object ctx) {
	            Server svr = (Server) ctx;
	            switch (KeeperException.Code.get(rc)) {
	                case CONNECTIONLOSS:
	                    removeSilence(svr);
	                    break;
	                case OK:
	                	LOGGER.info("Server {} re-enabled ({})", svr.getName(),
	                            svr.getServerAddress().getHostname());
	                    break;
	                default:
	                	LOGGER.info("[{}] Unexpected result for silencing {} ({})",
	                            new Object[]{KeeperException.Code.get(rc), svr.getName(), path});
	            }
	        }
	    };

	   
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
	    
	    protected String buildConfigPath(String fileName){
	    	return zkConfiguration.getPathMap().get(PathEnum.CONFIGPATH.name()) + PathUtil.FORWARD_SLASH + fileName;
	    }
	    
	public String buildPathChieldNode(String parentNode,String chieldNode) {
		return zkConfiguration.getPathMap().get(parentNode)
				+ PathUtil.FORWARD_SLASH + chieldNode;
	}
	
	public byte[] getNodeData(String pathNode) throws KeeperException, InterruptedException{
		 Stat stat = zk.exists(pathNode, this);
		 if(stat != null){
			 return zk.getData(pathNode, false, stat);
		 }
		 return null;
	}
		/**
		 * Delete all the nodes of zookeeper recursively
		 * 
		 * @param groupName
		 * @throws InterruptedException
		 * @throws KeeperException
		 * @throws Exception
		 */
		public void deleteAllNodes(String groupName) throws  Exception{ 
			try{
	    	ZKUtil.deleteRecursive(zk, groupName);
	    	LOGGER.info("All nodes are deleted");
	    	connectedSignal.await();
			}catch(InterruptedException | KeeperException e){
	    	LOGGER.info("\tUnable to delete the node Exception :: "+ e.getMessage());
	    	}
	    }
		
		/**
		 * Create the servers map which all need to be run on nodes
		 * 
		 */
		private void createServersMap() {
			List<String> checkUnique = new ArrayList<String>();
			Properties property = Property.loadServerProperties();
			int nodeIndex = 1;
			for(Object key : property.keySet()){
			String srv = property.getProperty((String)key);
			String IP = null;
			String PORT = null;
			
				IP = srv.split(":")[0];
				PORT = srv.split(":")[1];
				
			if (!checkUnique.contains(IP)) {
				Server server = new Server();
				server.setServerAddress(new ServerAddress(NODE_NAME_PRIFIX + nodeIndex++, IP));
				server.setPort(Integer.valueOf(PORT));
				server.setData(new Date().getTime());
				server.setServerType("simpleserver");
				server.setCurrentDateTime(ZKUtils.getCurrentTimeStamp());
				server.setDescription("A simple server to test monitoring");
				this.servers.add(server);
				checkUnique.add(IP);
				serverNameMap.put(server.getServerAddress().getHostname(), server);
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
				createNode(nodePath);
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
				 zk.delete(path, stat.getVersion(), deleteCallback, server);				 
				}
			} catch (KeeperException | InterruptedException e) {
				LOGGER.info("Unable to delete node :: " + server.getName() + " Exception is :: "+ e.getMessage());
				LOGGER.info(" PLEASE CHECK, ZOOKEEPER SERVER IS RUNNING or NOT!!");
			}
			
		}
		
		AsyncCallback.VoidCallback deleteCallback = new AsyncCallback.VoidCallback() {
	        @Override
	        public void processResult(int rc, String path, Object ctx) {
	            Server svr = (Server) ctx;
	            switch (KeeperException.Code.get(rc)) {
	                case CONNECTIONLOSS:
	                	deleteNode(svr);
	                	LOGGER.info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
	                    break;
	                case OK:
	                	LOGGER.info("Node {} is  ({})", svr.getName(),
	                            svr.getServerAddress().getHostname());
	                    break;
	                default:
	                	LOGGER.info("[{}] Unexpected result for deleting {} ({})",
	                            new Object[]{KeeperException.Code.get(rc), svr.getName(), path});
	            }
	        }
	    };
		
		public synchronized void checkZookeeperConnection(Server server){
			String path = buildMonitorPathForServer(server);
			zk.exists(path, this,checkStatusCallback,server);
		}
		
		AsyncCallback.StatCallback checkStatusCallback = new AsyncCallback.StatCallback() {
	        @Override
	        public void processResult(int rc, String path, Object ctx, Stat stat) {
	            Server svr = (Server) ctx;
	            switch (KeeperException.Code.get(rc)) {
	                case CONNECTIONLOSS:
	                	LOGGER.info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO CHECK STATUS...");
	                	checkZookeeperConnection(svr);
	                    break;
	                case OK:
	                	LOGGER.info("ZOOKEEPER SERVER IS RUNNING...");
	                	LOGGER.info("Node {} is  ({})", svr.getName(),
	                            svr.getServerAddress().getHostname());
	                    break;
	                default:
	                	LOGGER.info("[{}] Unexpected result for STATUS {} ({})",
	                            new Object[]{KeeperException.Code.get(rc), svr.getName(), path});
	            }
	        }
	    };
	    
	/**
	 * Save the file on ZNode of zookeeper
	 * 
	 * @param file
	 * @throws IOException
	 */
	public void saveConfigFileToZNode(ZKNodeFile file) throws IOException {
		createNode(buildConfigPath(file.getFileName()),
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
	
	public synchronized void deleteNode(String nodePath){
		 Stat stat = null;
		try {
			stat = zk.exists(nodePath, this);
			if(stat != null){
			 zk.delete(nodePath, stat.getVersion(), deleteCallback, nodePath);				 
			}
		} catch (KeeperException | InterruptedException e) {
			LOGGER.info("Unable to delete node :: " + nodePath + " Exception is :: "+ e.getMessage());
			LOGGER.info(" PLEASE CHECK, ZOOKEEPER SERVER IS RUNNING or NOT!!");
		}
		
	}
	
	AsyncCallback.VoidCallback deleteNodeCallback = new AsyncCallback.VoidCallback() {
       @Override
       public void processResult(int rc, String path, Object ctx) {
           String node = (String) ctx;
           switch (KeeperException.Code.get(rc)) {
               case CONNECTIONLOSS:
               	deleteNode(node);
               	LOGGER.info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
                   break;
               case OK:
               	LOGGER.info("Node {} is  ({})", node,path);
                   break;
               default:
               	LOGGER.info("[{}] Unexpected result for deleting {} ({})",
                           new Object[]{KeeperException.Code.get(rc), node, path});
           }
       }
   };
}
