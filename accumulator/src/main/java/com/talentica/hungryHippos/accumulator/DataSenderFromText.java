package com.talentica.hungryHippos.accumulator;

import com.talentica.hungryHippos.sharding.KeyCombination;
import com.talentica.hungryHippos.utility.marshaling.*;
import com.talentica.hungryHippos.sharding.Node;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by debasishc on 24/9/15.
 */
public class DataSenderFromText {
    private static String serverConfigFile = "serverConfigFile";

    private static String[] loadServers() throws Exception{
        ArrayList<String> servers = new ArrayList<>();
        BufferedReader in = new BufferedReader(
                new InputStreamReader(new FileInputStream(serverConfigFile)));
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
        try(ObjectInputStream in
                    = new ObjectInputStream(new FileInputStream("keyCombinationNodeMap"))){
            keyCombinationNodeMap = (Map<KeyCombination, Set<Node>>) in.readObject();
            //System.out.println(keyCombinationNodeMap);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        OutputStream[] targets = new OutputStream[servers.length];

        for(int i=0;i<targets.length;i++){
            String server = servers[i];
            System.out.println(server);
            Socket socket = new Socket(server,8080);
            targets[i] = new BufferedOutputStream(socket.getOutputStream(),8388608);
        }



        com.talentica.hungryHippos.utility.marshaling.FileReader
                input = new com.talentica.hungryHippos.utility.marshaling.FileReader(args[0]);
        input.setNumFields(9);
        input.setMaxsize(25);

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

            Map<String,Object> keyValueMap = new HashMap<>();
            keyValueMap.put("key1", key1);
            keyValueMap.put("key2", key2);
            keyValueMap.put("key3", key3);

            KeyCombination keyCombination = new KeyCombination(keyValueMap);
            dynamicMarshal.writeValueString(0, key1, byteBuffer);
            dynamicMarshal.writeValueString(1, key2, byteBuffer);
            dynamicMarshal.writeValueString(2, key3, byteBuffer);
            dynamicMarshal.writeValueString(3, key4, byteBuffer);
            dynamicMarshal.writeValueString(4, key5, byteBuffer);
            dynamicMarshal.writeValueString(5, key6, byteBuffer);
            dynamicMarshal.writeValueDouble(6, key7, byteBuffer);
            dynamicMarshal.writeValueDouble(7,key8, byteBuffer);
            dynamicMarshal.writeValue(8,"xyz",byteBuffer);
            for (Node node : keyCombinationNodeMap.get(keyCombination)) {
                targets[node.getNodeId()].write(buf);
            }



        }

        for(int j=0;j<targets.length;j++){
            targets[j].flush();
            targets[j].close();
        }
        long end = System.currentTimeMillis();

        System.out.println("Time taken in ms: "+(end-start));

    }
}
