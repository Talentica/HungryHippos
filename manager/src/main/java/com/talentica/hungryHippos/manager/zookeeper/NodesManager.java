/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.io.File;
import java.text.SimpleDateFormat;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.talentica.hungryHippos.manager.util.PathEnum;
import com.talentica.hungryHippos.manager.util.PathUtil;

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
    private static final int DEFAULT_NODES = 3;
    private Map<String,String> pathMap;
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
	     * 
	     * @throws Exception
	     */
	public void bootstrap(){
		createServersMap();
		createNode(PathUtil.SLASH+zkConfiguration.getPathMap().get(PathEnum.NAMESPACE.name()));
		createNode(zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()));
		createNode(zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()));
	}
	    /**
	     * Create the node on zookeeper
	     * 
	     * @param node
	     */
	    protected void createNode(final String node) {
	    	
	        zk.create(node, node.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
	                new AsyncCallback.StringCallback() {
	                    @Override
	                    public void processResult(int rc, String path, Object ctx, String name) {
	                        switch (KeeperException.Code.get(rc)) {
	                            case CONNECTIONLOSS:
	                                createNode(path);
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
	     * By adding a node with the server's hostname in the `alerts subtree`,
	     * we are effectively preventing any alerts to be triggered on this server.
	     * <p>This is useful when processing an alert, and wanting to avoid that more than one
	     * monitor triggers the alerting plugins; or it could be set (via an API call) to silence a
	     * "flaky" or "experimental" server that keeps triggering alerts.
	     * 
	     * @param server the server that will be 'silenced'
	     * @param persistent whether the silence should survive this server's session
	     * 		  failure/termination
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
	     * @param server
	     * @return
	     */
	    protected String buildAlertPathForServer(Server server) {
	        return buildAlertPathForServer(server.getServerAddress().getHostname());
	    }
	    
	    /**
	     * @param serverHostname
	     * @return
	     */
	    protected String buildAlertPathForServer(String serverHostname) {
	    	return zkConfiguration.getPathMap().get(PathEnum.ALERTPATH.name()) + PathUtil.SLASH + serverHostname;
	    }

	    /**
	     * @param server
	     * @return
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
			serverHostname = serverHostname.substring(serverHostname
					.lastIndexOf(File.separatorChar) + 1);
		}
		return zkConfiguration.getPathMap().get(PathEnum.BASEPATH.name()) + PathUtil.SLASH + serverHostname;
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
		
		private void createServersMap() {
			String serverIPs = prop.getProperty("node.server.ips");
			List<String> checkUnique = new ArrayList<String>();
			String IP = null;
			String PORT = null;
			String[] servers = serverIPs.split(",");
			int nodeIndex = 1;
			for (String str : servers) {
				IP = str.split(":")[0];
				PORT = str.split(":")[1];
				
			if (!checkUnique.contains(IP)) {
				Server server = new Server();
				server.setServerAddress(new ServerAddress(
						"_node" + nodeIndex++, IP));
				server.setPort(Integer.valueOf(PORT));
				server.setData(new Date().getTime());
				server.setServerType("simpleserver");
				server.setCurrentDateTime(getCurrentTimeStamp());
				server.setDescription("A simple server to test monitoring");
				this.servers.add(server);
				checkUnique.add(IP);
				serverNameMap.put(server.getServerAddress().getHostname(), server);
				LOGGER.info("\tSERVER NAME AND IP :: ["+server.getName()+" , "+server.getServerAddress().getIp()+"]");				
				}
			 getSignal = new CountDownLatch(this.servers.size()+DEFAULT_NODES);// additional 3 is added because of namesapce,alert and basepath node is also created
			}
		}
		
		private void createServerNodes() throws JsonProcessingException{			
			for(Server server : this.servers){
				String nodePath = buildMonitorPathForServer(server);
				createNode(nodePath);
			}
			
		}
		
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
	    
	    public static String getCurrentTimeStamp() {
	        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
	        Date now = new Date();
	        String strDate = sdfDate.format(now);
	        return strDate;
	    }
	    
}
