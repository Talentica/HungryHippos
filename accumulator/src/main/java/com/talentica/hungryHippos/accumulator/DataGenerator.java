package com.talentica.hungryHippos.accumulator;

import com.talentica.hungryHippos.sharding.KeyCombination;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by debasishc on 20/8/15.
 */
public class DataGenerator {


    public static long entryCount = 10_000_000;
    public final static char [] allChars;
    public final static char[] allNumbers;
    static {
        allChars = new char[26];
        for(int i=0;i<26;i++){
            allChars[i]=(char)('a'+i);
        }

        allNumbers = new char[10];
        for(int i=0;i<10;i++){
            allNumbers[i]=(char)('0'+i);
        }
    }
    public static String [] key1ValueSet
            = generateAllCombinations(1,allChars).toArray(new String[0]);

    public static String [] key2ValueSet
            = generateAllCombinations(1,allChars).toArray(new String[0]);

    public static String [] key3ValueSet
            = generateAllCombinations(1,allChars).toArray(new String[0]);

    public static String [] key4ValueSet
            = generateAllCombinations(1,allNumbers).toArray(new String[0]);

    public static String [] key5ValueSet
            = generateAllCombinations(1,allNumbers).toArray(new String[0]);

    public static String [] key6ValueSet
            = generateAllCombinations(2,allNumbers).toArray(new String[0]);

    private static List<String> generateAllCombinations(int numChars, char[] sourceChars){

        List<String> retList = new ArrayList<>();
        if(numChars<=0){

            retList.add("");
            return retList;
        }
        List<String> listForTheRest = generateAllCombinations(numChars - 1, sourceChars);
        for(char c:sourceChars){
            for(String source:listForTheRest){
                retList.add(c+source);
            }
        }
        return retList;
    }

    private static double skewRandom(){
        double start = Math.random();
        return start*start;
    }



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
            targets[i] =socket.getOutputStream();
        }

        long start = System.currentTimeMillis();

        for(int i=0;i<entryCount;i++){
            int i1 = (int)(key1ValueSet.length*skewRandom());
            int i2 = (int)(key2ValueSet.length*skewRandom());
            int i3 = (int)(key3ValueSet.length*skewRandom());
            int i4 = (int)(key4ValueSet.length*skewRandom());
            int i5 = (int)(key5ValueSet.length*skewRandom());
            int i6 = (int)(key6ValueSet.length*skewRandom());

            String key1 = key1ValueSet[i1];
            String key2 = key2ValueSet[i2];
            String key3 = key3ValueSet[i3];
            String key4 = key4ValueSet[i4];
            String key5 = key5ValueSet[i5];
            String key6 = key6ValueSet[i6];
            double key7 = Math.random() * key2.charAt(0);
            double key8 = Math.random() * key1.charAt(0);

            Map<String,Object> keyValueMap = new HashMap<>();
            keyValueMap.put("key1", key1);
            keyValueMap.put("key2", key2);
            keyValueMap.put("key3", key2);
            
            KeyCombination keyCombination = new KeyCombination(keyValueMap);
            dynamicMarshal.writeValue(0,key1,byteBuffer);
            dynamicMarshal.writeValue(1,key2,byteBuffer);
            dynamicMarshal.writeValue(2,key3,byteBuffer);
            dynamicMarshal.writeValue(3,key4,byteBuffer);
            dynamicMarshal.writeValue(4,key5,byteBuffer);
            dynamicMarshal.writeValue(5,key6,byteBuffer);
            dynamicMarshal.writeValue(6,key7,byteBuffer);
            dynamicMarshal.writeValue(7,key8,byteBuffer);
            dynamicMarshal.writeValue(8,"xyz",byteBuffer);
            for (Node node : keyCombinationNodeMap.get(keyCombination)) {
                targets[node.getNodeId()].write(buf);
            }



        }
        long end = System.currentTimeMillis();
        for(int j=0;j<targets.length;j++){
            targets[j].flush();
            targets[j].close();
        }

        System.out.println("Time taken in ms: "+(end-start));

    }
}
