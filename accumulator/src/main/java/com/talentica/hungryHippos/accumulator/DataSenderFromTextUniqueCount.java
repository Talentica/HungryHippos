package com.talentica.hungryHippos.accumulator;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.KeyCombination;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;
import com.talentica.hungryHippos.utility.marshaling.Reader;

/**
 * Created by debasishc on 12/10/15.
 */
public class DataSenderFromTextUniqueCount {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataSenderFromTextUniqueCount.class);

    private static String serverConfigFile = "serverConfigFile";

    private static String[] loadServers() throws Exception{
        ArrayList<String> servers = new ArrayList<>();        
        BufferedReader in = new BufferedReader(
                new InputStreamReader(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+serverConfigFile)));
        while(true){
            String line = in.readLine();
            if(line==null){
                break;
            }
            servers.add(line);
        }
        return servers.toArray(new String[servers.size()]);
    }



    public static void main(String [] args) throws Exception {
        long start = System.currentTimeMillis();

        String [] servers = loadServers();



        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,6);
        dataDescription.addFieldType(DataLocator.DataType.STRING,6);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);

        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});
        byte[] buf = new byte[dataDescription.getSize()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);


        Map<KeyCombination, Set<Node>> keyCombinationNodeMap = null;
        try(ObjectInputStream in
                    = new ObjectInputStream(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+"keyCombinationNodeMap"))){
            keyCombinationNodeMap = (Map<KeyCombination, Set<Node>>) in.readObject();
			// LOGGER.info(keyCombinationNodeMap);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        OutputStream[] targets = new OutputStream[servers.length];

        for(int i=0;i<targets.length;i++){
            String server = servers[i];
			LOGGER.info(server);
            Socket socket = new Socket(server,8080);
            targets[i] = new BufferedOutputStream(socket.getOutputStream(),8388608);
        }



        Reader
                input = new com.talentica.hungryHippos.utility.marshaling.FileReader(args[0]);
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
            MutableCharArrayString key5 = parts[4];
            MutableCharArrayString key6 = parts[5];
            MutableCharArrayString key7 = parts[6];
            MutableCharArrayString key8 = parts[7];;
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
            dynamicMarshal.writeValueString(6, key7, byteBuffer);
            dynamicMarshal.writeValueString(7, key8, byteBuffer);
            dynamicMarshal.writeValueString(8, key9,byteBuffer);
            //long endEncoding = System.currentTimeMillis();
            //timeForEncoding+=endEncoding-startEncoding;


            Set<Node> nodes = keyCombinationNodeMap.get(keyCombination);

            //long endLookp =System.currentTimeMillis();

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

		LOGGER.info("Time taken in ms: " + (end - start));
		LOGGER.info("Time taken in encoding: " + (timeForEncoding));
		LOGGER.info("Time taken in lookup: " + (timeForLookup));

    }
}

