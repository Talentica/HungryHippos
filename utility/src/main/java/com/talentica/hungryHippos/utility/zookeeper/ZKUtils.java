/**
 * 
 */
package com.talentica.hungryHippos.utility.zookeeper;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

/**
 * 
 * Zookeeper utility to perform various handy operation.
 * 
 * @author PooshanS
 *
 */
public class ZKUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class.getName());
	private static final String ZK_ROOT_NODE = "/rootnode";
	public static ZooKeeper zk;
	public static NodesManager nodesManager;
	
	public static ZKNodeFile getConfigZKNodeFile(
			String fileName) {
		Object obj = null;
		ZKNodeFile zkFile = null;
		try {
			obj = nodesManager
					.getConfigFileFromZNode(fileName);
			zkFile = (obj == null) ? null : (ZKNodeFile) obj;
		} catch (ClassNotFoundException | KeeperException
				| InterruptedException | IOException e) {
			e.printStackTrace();
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
	public static Object deserialize(byte[] obj) throws IOException,
			ClassNotFoundException {
	    	return SerializationUtils.deserialize(obj);
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
    public static Set<LeafBean> searchTree(String searchString, String authRole) throws InterruptedException, KeeperException, IOException, ClassNotFoundException {
        /*Export all nodes and then search.*/
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
    
    private static void exportTreeInternal(Set<LeafBean> entries, String path, String authRole) throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
        
        entries.addAll(listLeaves(path, authRole)); //List leaves
        
        /*Process folders*/
        for (String folder : listFolders(path)) {
            exportTreeInternal(entries, getNodePath(path, folder), authRole);
        }
    }

    public static List<LeafBean> listLeaves(String path, String authRole) throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
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
                    List<String> subChildren = zk.getChildren(path + PathUtil.FORWARD_SLASH + child, false);
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
        return path + PathUtil.FORWARD_SLASH + name;

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
	 */
	public static LeafBean getNodeValue(String path, String childPath,
			String child, String authRole) throws ClassNotFoundException, IOException {
		try {
			byte[] dataBytes = null;
			 Stat stat = zk.exists(childPath, nodesManager);
			 if(stat != null){
				 dataBytes =  zk.getData(childPath, false, stat);
			 }
			return (new LeafBean(path, child, dataBytes));
		} catch (KeeperException | InterruptedException ex) {
			LOGGER.error(ex.getMessage());
		}
		return null;

	}
	 
	 public static Object externalizeNodeValue(byte[] value) throws ClassNotFoundException, IOException {
		    if(value == null)
		    return null;
		 	return ZKUtils.deserialize(value);
	    }
	 
	 public static String buildNodePath(int nodeId){
			return Property.getProperties().getProperty("zookeeper.base_path") + PathUtil.FORWARD_SLASH + ("_node"+nodeId);
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
	 * @param signal
	 * @return
	 */
	public static AsyncCallback.StatCallback checkPathExistsStatusAsync(CountDownLatch signal) {
		AsyncCallback.StatCallback checkPathExistsCallback = new AsyncCallback.StatCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, Stat stat) {
				switch (KeeperException.Code.get(rc)) {
				case CONNECTIONLOSS:
					LOGGER.info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO CHECK STATUS...");
					break;
				case OK:
					LOGGER.info("ZOOKEEPER SERVER IS RUNNING...");
					break;
				case NONODE:
					try {
						nodesManager.isNodeExists(path, signal);
					} catch (KeeperException | InterruptedException e) {
						e.printStackTrace();
					}
					break;
				case NODEEXISTS:
					LOGGER.info("Node {} exists",path);
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
					LOGGER.info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
					break;
				case OK:
					LOGGER.info("Node {} is  ({})", node, path);
					break;
				default:
					LOGGER.info("[{}] Unexpected result for deleting {} ({})",
							new Object[] { KeeperException.Code.get(rc), node,
									path });
				}
			}
		};
		return deleteNodeCallback;
	}
		
	/**
	 * Check zookeeper connection Async on zookeeper
	 * @return
	 */
	public static AsyncCallback.StatCallback checkZKConnectionStatusAsync() {
		AsyncCallback.StatCallback checkStatusCallback = new AsyncCallback.StatCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, Stat stat) {
				Server svr = (Server) ctx;
				switch (KeeperException.Code.get(rc)) {
				case CONNECTIONLOSS:
					LOGGER.info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO CHECK STATUS...");
					nodesManager.checkZookeeperConnection(svr);
					break;
				case OK:
					LOGGER.info("ZOOKEEPER SERVER IS RUNNING...");
					LOGGER.info("Node {} is  ({})", svr.getName(), svr
							.getServerAddress().getHostname());
					break;
				default:
					LOGGER.info(
							"[{}] Unexpected result for STATUS {} ({})",
							new Object[] { KeeperException.Code.get(rc),
									svr.getName(), path });
				}
			}
		};
		return checkStatusCallback;
	}
	
	/**
	 * Check server delete status Async on zookeeper
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
					LOGGER.info("ZOOKEEPER CONNECTION IS LOST/ZOOKEEPER IS NOT RUNNING. RETRYING TO DELETE...");
					break;
				case OK:
					LOGGER.info("Node {} is  ({})", svr.getName(), svr
							.getServerAddress().getHostname());
					break;
				default:
					LOGGER.info(
							"[{}] Unexpected result for deleting {} ({})",
							new Object[] { KeeperException.Code.get(rc),
									svr.getName(), path });
				}
			}
		};
		return deleteCallback;
	}
	
	/**
	 * Check romove status of Alert Async on zookeeper
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
					LOGGER.info("Server {} re-enabled ({})", svr.getName(), svr
							.getServerAddress().getHostname());
					break;
				default:
					LOGGER.info(
							"[{}] Unexpected result for alerting {} ({})",
							new Object[] { KeeperException.Code.get(rc),
									svr.getName(), path });
				}
			}
		};
		return removeAlertCallback;
	}
	
	
	/**
	 * Check Alert create Async on zookeeper
	 * @return
	 */
	public static AsyncCallback.StringCallback checkCreateAlertStatusAsync() {
		AsyncCallback.StringCallback createAlertCallback = new AsyncCallback.StringCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx,
					String name) {

				Server svr = (Server) ctx;
				switch (KeeperException.Code.get(rc)) {
				case CONNECTIONLOSS:
					nodesManager.registerAlert(svr, false);
					break;
				case NODEEXISTS:
					LOGGER.info("Trying to alert an already silenced server ["
							+ name + "]");
					break;
				case OK:
					LOGGER.info("Server {} silenced ({})", svr.getName(), svr
							.getServerAddress().getHostname());
					try {
						if (nodesManager.getMonitoredServers() != null) {
							LOGGER.info(
									"STATUS :: NOW, There are currently {} "
											+ "servers: {}", nodesManager
											.getMonitoredServers().size(),
									nodesManager.getMonitoredServers()
											.toString());
						}
					} catch (Exception e) {
						LOGGER.info("Unable to get the monitored servers");
					}
					break;
				default:
					LOGGER.info(
							"[{}] Unexpected result for alerting {} ({})",
							new Object[] { KeeperException.Code.get(rc),
									svr.getName(), path });
				}
			}
		};
		return createAlertCallback;
	}
}
