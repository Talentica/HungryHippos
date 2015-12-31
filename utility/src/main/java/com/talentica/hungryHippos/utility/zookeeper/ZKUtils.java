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

import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.KeeperException;
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
	 
}
