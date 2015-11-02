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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
    private String namespacePath;
    private List<Server> servers;
    private Map<String,Server> serverNameMap;
    private Properties prop;
    
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
		prop = new Property().getProperties();
		namespacePath = prop.getProperty("namespace.path");
		String hosts = prop.getProperty("zookeeper.server.ips");
		String basePath = prop.getProperty("zookeeper.base_path");
		String alertPath = prop.getProperty("zookeeper.alerts_path");
		Integer sessionTimeOut = Integer.valueOf(prop
				.getProperty("zookeeper.session_timeout"));
		String confPath = prop.getProperty("zookeeper.config_path");
		zkConfiguration = new ZookeeperConfiguration(hosts, basePath,
				alertPath, sessionTimeOut, confPath,namespacePath);
		connectZookeeper();
	}

	public void connectZookeeper() {
		System.out.println("\n\tNode Manager started, connecting to ZK hosts: "
				+ zkConfiguration.getHosts());
		try {
			connect(zkConfiguration.getHosts());
			System.out.printf("\n\tConnected - Session ID: " + zk.getSessionId());
		} catch (Exception e) {
			System.out.printf("Could not connect to Zookeper instance", e);
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
			System.out.printf("\n\tStarting Node Manager.. \n\tAlready the server/servers {%s} running!",serverNames);
			for (String name : serverNames) {
				Server server = getServerInfo(name);
				if (server != null)
					registrationListener.register(server);
			}
		}catch(KeeperException non){
	        	System.out.println("No node exists!!");
	        }
		 createServerNodes();
		 if(registrationListener != null){
	        System.out.printf(String.format("\n\tNodes Manager Started successfully.. \n\tNOW ,There are currently %d " +
	                "servers: %s", registrationListener.getRegisteredServers().size(), registrationListener.getRegisteredServers().toString()));
		 }
		 getSignal.await();
	    }

	    /**
	     * Bootstrap
	     * 
	     * @throws Exception
	     */
	public void bootstrap(){
		createNode(PathUtil.SLASH+zkConfiguration.getNameSpace());
		createNode(zkConfiguration.getAlertsPath());
		createNode(zkConfiguration.getBasePath());
		createServers();
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
	                                System.out.println("\n\tServer Monitoring Path [" + path + "] is created");
	                                getSignal.countDown();
	                                System.out.println(Thread.currentThread().getName());
	                                break;
	                            case NODEEXISTS:
	                            	System.out.println("\n\tServer Monitoring Path [" + path + "] already exists");
	                            	getSignal.countDown();
	                            	System.out.println(Thread.currentThread().getName());
	                                break;
	                            default:
	                            	System.out.println("\n\tUnexpected result while trying to create node " +
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
			System.out.println(String.format(
					"\n\t\n\tNodeManager watched event [%s] for %s",
					watchedEvent.getType(), watchedEvent.getPath()));
		}
	        switch (watchedEvent.getType()) {
	            case None:
	                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
	                	System.out.println("\n\tZooKeeper client connected to server");
	                    connectedSignal.countDown();
	                }
	                break;
	            case NodeChildrenChanged:
	                try {
	                	    System.out.println(Thread.currentThread().getName());
	                		List<String> servers = zk.getChildren(zkConfiguration.getBasePath(), this);
	                		Collections.sort(servers);
		                    System.out.printf(String.format("\n\tServer modified. Watched servers NOW: %s",
		                            servers.toString()));
		                    Map<String, Set<Server>> diffs = computeDiff(servers);
		                    System.out.println(String.format("\n\tRemoved Servers: %s", diffs.get(REMOVED)));
		                    System.out.println(String.format("\n\tAdded servers: %s", diffs.get(ADDED)));
		                    	 processDiffs(diffs);
		                    	
	                    
	                } catch (KeeperException | InterruptedException e) {
	                	System.out.printf(String.format("\n\tThere was an error retrieving the list of " +
	                            "servers [%s]", e.getLocalizedMessage()), e);
	                }
	                break;
	            case NodeDataChanged:
	            	System.out.println(Thread.currentThread().getName());
	            	System.out.println(String.format("\n\tA node changed: %s [%s]", watchedEvent.getPath(),
	                        watchedEvent.getState()));
	                try {
	                    Server server = getServerInfo(watchedEvent.getPath());
	                    registrationListener.updateServer(server);
	                    zk.exists(watchedEvent.getPath(), this);
	                } catch (KeeperException | InterruptedException ex) {
	                	System.out.printf(String.format("\n\tThere was an error while updating the data " +
	                            "associated with node %s: %s", watchedEvent.getPath(),
	                            ex.getLocalizedMessage()));
	                }
	                break;
	            case NodeDeleted:
	            	System.out.println(Thread.currentThread().getName());
	            	System.out.printf("\n\tA node is deleted: %s ",watchedEvent.getPath());
	            	break;
	            case NodeCreated:
	            	System.out.println(Thread.currentThread().getName());
	            	System.out.printf("\n\t A node is created: %s",watchedEvent.getPath());
	            	break;
	            default:
	            	System.out.printf(String.format("\n\tNot an expected event: %s; for %s",
	                        watchedEvent.getType(), watchedEvent.getPath()));
	        }
	    }

	    /**
	     * To process the difference of the server for added and removed
	     * 
	     * @param diffs
	     */
	    private synchronized void processDiffs(Map<String, Set<Server>> diffs) {
	    	 for (Server server : diffs.get(REMOVED)) {
		        	System.out.println("\n\tReporting eviction to listener: " +
		                    server.getServerAddress().getHostname());
		        	evictionListener.deregister(server);
		            silence(server, false);
		        }
	        for (Server server : diffs.get(ADDED)) {
	        	System.out.println("\n\tReporting addition to listener: " +
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
	    	List<String> serverList = zk.getChildren(zkConfiguration.getBasePath(), this);
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
	            	System.out.println("\n\tServer " + server.getName() + " has been removed");
	                evictedServers.add(server);
	            }
	        }
	        Set<Server> newlyAddedServers = Sets.newHashSet();
	        for (String name : latestServersNames) {
	        	System.out.println("\n\tServer " + name + " has joined the monitored pool");
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
			System.out.printf(String.format("\n\tServer: %s -- Payload: %s", server.getName(),
			        server.getData()));
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
	            String msg = String.format("Caught an exception while trying to silence %s (%s)",
	                    server, e.getLocalizedMessage());
	            System.out.println(msg);
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
	                	System.out.println("\n\tTrying to silence an already silenced server [" + name + "]");
	                    break;
	                case OK:
	                	System.out.println(String.format("\n\tServer %s silenced (%s)", svr.getName(),
	                            svr.getServerAddress().getHostname()));	                   
	                    	try {
	                    		  if(getMonitoredServers() != null){
	                    			System.out.printf(String.format(
	                    						"\n\tSTATUS :: NOW, There are currently %d "
	                    						+ "servers: %s", getMonitoredServers().size(), getMonitoredServers().toString()));	   
	                    		  }
	                    		} catch (Exception e) {
	                    			System.out.println("Unable to get the monitored servers");
	                    		}
	                    break;
	                default:
	                	System.out.println(String.format("\n\t[%s] Unexpected result for silencing %s (%s)",
	                            KeeperException.Code.get(rc), svr.getName(), path));
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
	            	System.out.println("\n\tRemoved silence for node: " + server.getName());
	            }
	        } catch (KeeperException | InterruptedException kex) {
	        	System.out.println(String.format("\n\tException encountered ('%s') while pruning the %s " +
	                    "branch, upon registration of %s - this is probably safe to ignore, " +
	                    "unless other unexplained errors start to crop up",
	                    kex.getLocalizedMessage(), path, server.getName()));
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
	                	System.out.printf(String.format("\n\tServer %s re-enabled (%s)", svr.getName(),
	                            svr.getServerAddress().getHostname()));
	                    break;
	                default:
	                	System.out.printf(String.format("\n\t[%s] Unexpected result for silencing %s (%s)",
	                            KeeperException.Code.get(rc), svr.getName(), path));
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
	    	return zkConfiguration.getAlertsPath() + PathUtil.SLASH + serverHostname;
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
		if (serverHostname.startsWith(zkConfiguration.getBasePath())) {
			return serverHostname;
		} else if (serverHostname.startsWith(zkConfiguration.getAlertsPath())) {
			serverHostname = serverHostname.substring(serverHostname
					.lastIndexOf(File.separatorChar) + 1);
		}
		return zkConfiguration.getBasePath() + PathUtil.SLASH + serverHostname;
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
			System.out.println();
			try{
	    	ZKUtil.deleteRecursive(zk, groupName);
	    	System.out.println("\n\tAll nodes are deleted");
	    	connectedSignal.await();
			}catch(InterruptedException | KeeperException e){
	    	System.out.println("\tUnable to delete the node Exception :: "+ e.getMessage());
	    	}
	    }
		
		private void createServers() {
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
				System.out.println("\tSERVER NAME AND IP :: ["+server.getName()+" , "+server.getServerAddress().getIp()+"]");				
				}
			  
			}
		}
		
		private void createServerNodes() throws JsonProcessingException{			
			 getSignal = new CountDownLatch(this.servers.size());
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
				System.out.println("\n\tUnable to delete node :: " + server.getName() + " Exception is :: "+ e.getMessage());
				System.out.println("\n\t PLEASE CHECK, ZOOKEEPER SERVER IS RUNNING or NOT!!");
			}
			
		}
		
		AsyncCallback.VoidCallback deleteCallback = new AsyncCallback.VoidCallback() {
	        @Override
	        public void processResult(int rc, String path, Object ctx) {
	            Server svr = (Server) ctx;
	            switch (KeeperException.Code.get(rc)) {
	                case CONNECTIONLOSS:
	                	deleteNode(svr);
	                	System.out.println("\n\tZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
	                    break;
	                case OK:
	                	System.out.printf(String.format("\n\tNode %s is  (%s)", svr.getName(),
	                            svr.getServerAddress().getHostname()));
	                    break;
	                default:
	                	System.out.printf(String.format("\n\t[%s] Unexpected result for deleting %s (%s)",
	                            KeeperException.Code.get(rc), svr.getName(), path));
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
	                	System.out.println("\n\tZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO CHECK STATUS...");
	                	checkZookeeperConnection(svr);
	                    break;
	                case OK:
	                	System.out.println("ZOOKEEPER SERVER IS RUNNING...");
	                	System.out.printf(String.format("\n\tNode %s is  (%s)", svr.getName(),
	                            svr.getServerAddress().getHostname()));
	                    break;
	                default:
	                	System.out.printf(String.format("\n\t[%s] Unexpected result for STATUS %s (%s)",
	                            KeeperException.Code.get(rc), svr.getName(), path));
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
