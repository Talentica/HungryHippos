package com.talentica.hungryHippos.accumulator;

import java.io.BufferedOutputStream;
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
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;
import com.talentica.hungryHippos.utility.zookeeper.ZKNodeFile;
import com.talentica.hungryHippos.utility.zookeeper.ZKUtils;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

/**
 * Created by debasishc on 24/9/15.
 */
public class DataSenderFromText {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataSenderFromText.class.getName());
	private static String inputFile =  "sampledata.txt";
	
	
    private static String[] loadServers(NodesManager nodesManager) throws Exception{
    	LOGGER.info("Load the server form the configuration file");
        ArrayList<String> servers = new ArrayList<>();
        Object obj = nodesManager.getConfigFileFromZNode(Property.SERVER_CONF_FILE);
        ZKNodeFile serverConfig =  (obj == null) ? null : (ZKNodeFile)obj ;
        Properties prop =  serverConfig.getFileData();
        for(Object server : prop.keySet()){
        	servers.add(prop.getProperty((String)server));
        }
        return servers.toArray(new String[servers.size()]);
    }



    public static void main(String [] args) throws Exception {
    	//publishDataToNodes();    	
    }
    
    @SuppressWarnings("unchecked")
	public static void publishDataToNodes(NodesManager nodesManager) throws Exception{
    	
        long start = System.currentTimeMillis();

        String [] servers = loadServers(nodesManager);



        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.DOUBLE,0);
        dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 0);
        dataDescription.addFieldType(DataLocator.DataType.STRING, 4);

        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});
        byte[] buf = new byte[dataDescription.getSize()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);


        Map<KeyCombination, Set<Node>> keyCombinationNodeMap = null;
        ZKNodeFile mapfile = ZKUtils.getConfigZKNodeFile(ZKNodeName.keyCombinationNodeMap);
        keyCombinationNodeMap = (mapfile==null) ? null : (Map<KeyCombination, Set<Node>>)mapfile.getObj();
       
        OutputStream[] targets = new OutputStream[servers.length];

        for(int i=0;i<targets.length;i++){
            String server = servers[i];
            System.out.println(server);
            Socket socket = new Socket(server.split(":")[0].trim(),Integer.valueOf(server.split(":")[1].trim()));
            targets[i] = new BufferedOutputStream(socket.getOutputStream(),8388608);
        }



        com.talentica.hungryHippos.utility.marshaling.FileReader
                input = new com.talentica.hungryHippos.utility.marshaling.FileReader(inputFile);
        input.setNumFields(9);
        input.setMaxsize(25);

        long timeForEncoding = 0;
        long timeForLookup = 0;

        while(true){
            MutableCharArrayString[] parts = input.readCommaSeparated();
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
            dynamicMarshal.writeValueString(8, key9,byteBuffer);
            //long endEncoding = System.currentTimeMillis();
            //timeForEncoding+=endEncoding-startEncoding;


            Set<Node> nodes = keyCombinationNodeMap.get(keyCombination);

            //long endLookp =System.currentTimeMillis();
            //System.out.println("Size of array :: " + targets.length);
            //timeForLookup += endLookp - endEncoding;
            for (Node node : nodes) {
            	//System.out.println("Node Id :: " + node.getNodeId());
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
