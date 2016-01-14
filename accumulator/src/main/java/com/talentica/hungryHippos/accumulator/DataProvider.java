package com.talentica.hungryHippos.accumulator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.KeyCombination;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;
import com.talentica.hungryHippos.utility.zookeeper.ZKNodeFile;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

/**
 * Created by debasishc on 24/9/15.
 */
public class DataProvider {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class.getName());
	private static Map<KeyCombination, Set<Node>> keyCombinationNodeMap;
    private static Map<Integer,String> loadServers(NodesManager nodesManager) throws Exception{
    	LOGGER.info("Load the server form the configuration file");
        //ArrayList<String> servers = new ArrayList<>();
        Map<Integer,String> nodeIdServerMap = new ConcurrentHashMap<Integer,String>();
        
        Object obj = nodesManager.getConfigFileFromZNode(Property.SERVER_CONF_FILE);
        ZKNodeFile serverConfig =  (obj == null) ? null : (ZKNodeFile)obj ;
        Properties prop =  serverConfig.getFileData();
        for(Object server : prop.keySet()){
        	String serverName = (String) server;
        	int nodeId = Integer.valueOf(serverName.split("\\.")[1]);
        	nodeIdServerMap.put(nodeId, prop.getProperty(serverName));
        }
        LOGGER.info("There are {} servers",nodeIdServerMap.size());
        //return servers.toArray(new String[servers.size()]);
        return nodeIdServerMap;
    }


    public static void main(String [] args) throws Exception {
    	//publishDataToNodes();    	
    }
    
    @SuppressWarnings({ "unchecked", "resource" })
	public static void publishDataToNodes(NodesManager nodesManager){
    	
        long start = System.currentTimeMillis();

        Map<Integer, String> serversMap;
		try {
			serversMap = loadServers(nodesManager);
		} catch (Exception e1) {
			LOGGER.info("Unable to load servers");
			return;
		}
        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        CommonUtil.setDataDescription(dataDescription);
        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});
        byte[] buf = new byte[dataDescription.getSize()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);

		try (ObjectInputStream inKeyCombinationNodeMap = new ObjectInputStream(
				new FileInputStream(
						new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
								+ PathUtil.FORWARD_SLASH
								+ ZKNodeName.keyCombinationNodeMap))) {
			keyCombinationNodeMap = (Map<KeyCombination, Set<Node>>) inKeyCombinationNodeMap
					.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}

        OutputStream[] targets = new OutputStream[serversMap.size()];
        LOGGER.info("***CREATE SOCKET CONNECTIONS***");
        CountDownLatch count = new CountDownLatch(serversMap.size());
		while (count.getCount() != 0l) {
			for (Integer nodeId : serversMap.keySet()) {
				try {
					String server = serversMap.get(nodeId);
					Socket socket = new Socket(server.split(":")[0].trim(),
							Integer.valueOf(server.split(":")[1].trim()));
					targets[nodeId] = new BufferedOutputStream(
							socket.getOutputStream(), 8388608);
					if(count.getCount() != 0) count.countDown();
					serversMap.remove(nodeId);
				} catch (Exception cex) {
					LOGGER.warn("Exception is {}",cex.getMessage());
					LOGGER.warn(
							"Connection could not get established. Please start the node {}",
							serversMap.get(nodeId).split(":")[0].trim());
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						LOGGER.info("Unable to sleep the current thread");
					}
				}
				LOGGER.info("Total count {}",count.getCount());
			}
		}
        int k = 0;
        /*for(int i=0;i<targets.length;i++){
        	i = i-k;
        	k = 0; 
        	String server = servers.get(i);
        	try{           
            Socket socket = new Socket(server.split(":")[0].trim(),Integer.valueOf(server.split(":")[1].trim()));
            targets[i] = new BufferedOutputStream(socket.getOutputStream(),8388608);
        	}catch(ConnectException cex){
        		LOGGER.warn("Connection could not get established. Please start the node {}",server.split(":")[0].trim());
        		k = 1;
        		Thread.sleep(2000);
        	}
        }*/
        
        /*int index = 0;
		for (String server : servers) {
			try {
				Socket socket = new Socket(server.split(":")[0].trim(),Integer.valueOf(server.split(":")[1].trim()));
				targets[index] = new BufferedOutputStream(socket.getOutputStream(), 8388608);
				index++;
				servers.remove(server);
			} catch (ConnectException cex) {
				LOGGER.warn("Connection could not get established. Please start the node {}",server.split(":")[0].trim());
				Thread.sleep(2000);
			}
		}*/
		LOGGER.info("Total servers {}",targets.length);
        LOGGER.info("PUBLISH DATA ACROSS THE NODES STARTED...");
        com.talentica.hungryHippos.utility.marshaling.FileReader input = null;
		try {
			input = new com.talentica.hungryHippos.utility.marshaling.FileReader(Property.getProperties().getProperty("input.file"));
		} catch (IOException e) {
			LOGGER.info("Unable to read file");
			return;
		}
        input.setNumFields(9);
        input.setMaxsize(25);

        long timeForEncoding = 0;
        long timeForLookup = 0;

        while(true){
            MutableCharArrayString[] parts;
			try {
				parts = input.readCommaSeparated();
			} catch (IOException e) {
				LOGGER.info("Unable to read file");
				continue;
			}
            if(parts == null){
                break;
            }
            MutableCharArrayString key1 = parts[0];
            MutableCharArrayString key2 = parts[1];
            MutableCharArrayString key3 = parts[2];
            MutableCharArrayString key4 = parts[3];
            MutableCharArrayString key5 = parts[4];;
            MutableCharArrayString key6 = parts[5];;
            double key7 = Double.parseDouble(parts[6].toString());
            double key8 = Double.parseDouble(parts[7].toString());
            MutableCharArrayString key9 = parts[8];

            Map<String,Object> keyValueMap = new HashMap<>();
            keyValueMap.put("key1", key1);
            keyValueMap.put("key2", key2);
            keyValueMap.put("key3", key3);

            //long startEncoding = System.currentTimeMillis();

            KeyCombination keyCombination = new KeyCombination(keyValueMap);
            dynamicMarshal.writeValueString(0, key1, byteBuffer);
            dynamicMarshal.writeValueString(1, key2, byteBuffer);
            dynamicMarshal.writeValueString(2, key3, byteBuffer);
            dynamicMarshal.writeValueString(3, key4, byteBuffer);
            dynamicMarshal.writeValueString(4, key5, byteBuffer);
            dynamicMarshal.writeValueString(5, key6, byteBuffer);
            dynamicMarshal.writeValueDouble(6, key7, byteBuffer);
            dynamicMarshal.writeValueDouble(7, key8, byteBuffer);
            dynamicMarshal.writeValueString(8, key9, byteBuffer);
            //long endEncoding = System.currentTimeMillis();
            //timeForEncoding+=endEncoding-startEncoding;


            Set<Node> nodes = keyCombinationNodeMap.get(keyCombination);

            //long endLookp =System.currentTimeMillis();
            //System.out.println("Size of array :: " + targets.length);
            //timeForLookup += endLookp - endEncoding;
            //LOGGER.info("TOTAL NODES {}",nodes.size());
            for (Node node : nodes) {
            	//LOGGER.info("NODE ID {}",node.getNodeId());
                try {
					targets[node.getNodeId()].write(buf);
				} catch (IOException e) {
					LOGGER.info("Unable to write the file");
				}
            }

        }

        for(int j=0;j<targets.length;j++){
            try {
				targets[j].flush();
				targets[j].close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
           
        }
       
        long end = System.currentTimeMillis();

        System.out.println("Time taken in ms: "+(end-start));
        System.out.println("Time taken in encoding: "+(timeForEncoding));
        System.out.println("Time taken in lookup: "+(timeForLookup));

    
    }
}
