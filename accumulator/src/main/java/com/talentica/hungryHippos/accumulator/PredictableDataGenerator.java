package com.talentica.hungryHippos.accumulator;

import com.talentica.hungryHippos.sharding.KeyCombination;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by debasishc on 11/9/15.
 */
public class PredictableDataGenerator {

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

    public static void main(String [] args) throws Exception{
        Socket socket = new Socket("localhost",8080);
        //PrintWriter out = new PrintWriter(new File("sampledata.txt"));

        OutputStream out = socket.getOutputStream();

        FileInputStream in = new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+"predictableData");

        byte[] buffer = new byte[1024];
        int totalData=0;
        while(true){
            int read = in.read(buffer);

            if(read<0){
                break;
            }
            totalData+=read;
            out.write(buffer,0,read);
        }

        out.flush();
        out.close();
        System.out.println("Total data sent: "+totalData);
    }

    public static void main() throws Exception {

        BufferedReader i = new BufferedReader(new InputStreamReader(System.in));

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
                    = new ObjectInputStream(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+"keyCombinationNodeMap"))){
            keyCombinationNodeMap = (Map<KeyCombination, Set<Node>>) in.readObject();
            System.out.println(keyCombinationNodeMap);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Socket socket = new Socket("localhost",8080);
        //PrintWriter out = new PrintWriter(new File("sampledata.txt"));

        OutputStream out = new FileOutputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+"predictableData"); //socket.getOutputStream();
        long start = System.currentTimeMillis();
        System.out.println(generateAllCombinations(3,allNumbers));
        int count=0;
        for(KeyCombination kc:keyCombinationNodeMap.keySet()){

            int i4 = (int)(key4ValueSet.length*skewRandom());
            int i5 = (int)(key5ValueSet.length*skewRandom());
            int i6 = (int)(key6ValueSet.length*skewRandom());

            String key1 = (String)kc.getKeyValueCombination().get("key1");
            String key2 = (String)kc.getKeyValueCombination().get("key2");
            String key3 = (String)kc.getKeyValueCombination().get("key3");
            String key4 = key4ValueSet[i4];
            String key5 = key5ValueSet[i5];
            String key6 = key6ValueSet[i6];
            double key7 = Math.random();
            double key8 = Math.random();

            Map<String,Object> keyValueMap = new HashMap<>();
            keyValueMap.put("key1", key1);
            keyValueMap.put("key2", key2);
            keyValueMap.put("key3", key2);

            KeyCombination keyCombination = kc;//new KeyCombination(keyValueMap);
            Node targetNode = new Node(0,6);

            if(keyCombinationNodeMap.get(keyCombination).contains(targetNode)){
                System.out.println(kc);
                count++;
                dynamicMarshal.writeValue(0,key1,byteBuffer);
                dynamicMarshal.writeValue(1,key2,byteBuffer);
                dynamicMarshal.writeValue(2,key3,byteBuffer);
                dynamicMarshal.writeValue(3,key4,byteBuffer);
                dynamicMarshal.writeValue(4,key5,byteBuffer);
                dynamicMarshal.writeValue(5,key6,byteBuffer);
                dynamicMarshal.writeValue(6,key7,byteBuffer);
                dynamicMarshal.writeValue(7,key8,byteBuffer);
                dynamicMarshal.writeValue(8, "xyz", byteBuffer);
                out.write(buf);
                //i.readLine();

                //System.out.println("Wrote "+Arrays.toString(buf));
            }else{
                //System.out.println(keyCombination + "  " +keyCombinationNodeMap.get(keyCombination));
            }



        }
        long end = System.currentTimeMillis();
        out.flush();
        out.close();
        System.out.println("Time taken in ms: " + (end - start));
        System.out.println("Total number of rows sent: "+count);

    }
}
