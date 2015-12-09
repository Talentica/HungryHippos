/**
 * 
 */
package com.talentica.hungryHippos.utility.zookeeper;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	public static ZKNodeFile ZKNodeFile(NodesManager nodesManager,
			String fileName) {
		LOGGER.info("\n\tGetting value from node");
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
		LOGGER.info("\n\tSerialize the the object");
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
		LOGGER.info("\n\tDeserialize the the object");
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
    
    
    
}
