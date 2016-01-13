package com.talentica.hungryHippos.accumulator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import com.talentica.hungryHippos.utility.marshaling.Reader;
import com.talentica.hungryHippos.utility.server.ServerUtils;
import com.talentica.hungryHippos.utility.zookeeper.ZKNodeFile;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

/**
 * Created by debasishc on 24/9/15.
 */
public class DataProvider {
	
	private static final int NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE=10;
	private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class.getName());
	private static Map<KeyCombination, Set<Node>> keyCombinationNodeMap;
    private static String[] loadServers(NodesManager nodesManager) throws Exception{
    	LOGGER.info("Load the server form the configuration file");
        ArrayList<String> servers = new ArrayList<>();
        Object obj = nodesManager.getConfigFileFromZNode(Property.SERVER_CONF_FILE);
        ZKNodeFile serverConfig =  (obj == null) ? null : (ZKNodeFile)obj ;
        Properties prop =  serverConfig.getFileData();
        for(Object server : prop.keySet()){
        	servers.add(prop.getProperty((String)server));
        }
        LOGGER.info("There are {} servers",servers.size());
        return servers.toArray(new String[servers.size()]);
    }


    public static void main(String [] args) throws Exception {
    	//publishDataToNodes();    	
    }
    
    @SuppressWarnings({ "unchecked", "resource" })
	public static void publishDataToNodes(NodesManager nodesManager) throws Exception{
    	
        long start = System.currentTimeMillis();

        String [] servers = loadServers(nodesManager);
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

        OutputStream[] targets = new OutputStream[servers.length];
        LOGGER.info("***CREATE SOCKET CONNECTIONS***");
        
		for (int i = 0; i < servers.length; i++) {
			String server = servers[i];
			Socket socket = ServerUtils.connectToServer(server, NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE);
			targets[i] = new BufferedOutputStream(socket.getOutputStream(), 8388608);
		}

        LOGGER.info("\n\tPUBLISH DATA ACROSS THE NODES STARTED...");
        Reader
                input = new com.talentica.hungryHippos.utility.marshaling.FileReader(Property.getProperties().getProperty("input.file"));
        input.setNumFields(9);
        input.setMaxsize(25);

        long timeForEncoding = 0;
        long timeForLookup = 0;

        while(true){
            MutableCharArrayString[] parts = input.read();
            if(parts == null){
				input.close();
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
            for (Node node : nodes) {
                targets[node.getNodeId()].write(buf);
            }

        }

        for(int j=0;j<targets.length;j++){
            targets[j].flush();
            targets[j].close();
        }
       
        long end = System.currentTimeMillis();

        System.out.println("Time taken in ms: "+(end-start));
        System.out.println("Time taken in encoding: "+(timeForEncoding));
        System.out.println("Time taken in lookup: "+(timeForLookup));

    
    }


}
