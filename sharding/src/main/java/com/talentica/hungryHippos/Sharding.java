package com.talentica.hungryHippos;

import java.io.*;
import java.util.*;

/**
 * Created by debasishc on 14/8/15.
 */
public class Sharding {
    private Map<String,List<KeyValueFrequency>> keyValueFrequencyMap = new HashMap<>();
    private Map<String,Map<Object, Node>> keyValueNodeNumberMap = new HashMap<>();
    PriorityQueue<Node> fillupQueue = new PriorityQueue<>(new NodeRemainingCapacityComparator());
    private Map<KeyCombination, Long> keyCombinationFrequencyMap = new HashMap<>();
    private Map<Node, List<KeyCombination>> nodeToKeyMap = new HashMap<>();
    private Map<KeyCombination, Set<Node>> keyCombinationNodeMap = new HashMap<>();

    public Sharding(int numNodes) {
        for(int i=0;i<numNodes;i++){
            Node node = new Node(300000,i);
            fillupQueue.offer(node);
            nodeToKeyMap.put(node, new  ArrayList<KeyCombination>());
        }
    }

    private long sumForKeyCombinationIntersection(KeyCombination keyCombination){
        long sum=0;
        for(Map.Entry<KeyCombination,Long> entry: keyCombinationFrequencyMap.entrySet()){
            KeyCombination keyCombination1 = entry.getKey();
            if(keyCombination.checkMatchAnd(keyCombination1)){
                sum+=entry.getValue();
            }
        }
        return sum;
    }

    private long sumForKeyCombinationUnion(KeyCombination keyCombination){
        long sum=0;
        for(Map.Entry<KeyCombination,Long> entry: keyCombinationFrequencyMap.entrySet()){
            KeyCombination keyCombination1 = entry.getKey();
            if(keyCombination.checkMatchOr(keyCombination1)){
                sum+=entry.getValue();
            }
        }
        return sum;
    }

    private long sumForKeyCombinationUnion(List<KeyCombination> keyCombination){
        long sum=0;
        for(Map.Entry<KeyCombination,Long> entry: keyCombinationFrequencyMap.entrySet()){
            KeyCombination keyCombination1 = entry.getKey();
            if(keyCombination1.checkMatchOr(keyCombination)){
                sum+=entry.getValue();
            }
        }
        return sum;
    }


    private void shardSingleKey(String keyName) throws NodeOverflowException {
        List<KeyValueFrequency> keyValueFrequencies = keyValueFrequencyMap.get(keyName);
        Map<Object, Node> keyValueNodeNumber =  new HashMap<>();
        keyValueNodeNumberMap.put(keyName, keyValueNodeNumber);
        Collections.sort(keyValueFrequencies);
        for(KeyValueFrequency kvf:keyValueFrequencies){
            Node mostEmptyNode = fillupQueue.poll();

            List<KeyCombination> currentKeys = nodeToKeyMap.get(mostEmptyNode);
            if(currentKeys==null){
                currentKeys = new ArrayList<>();
                nodeToKeyMap.put(mostEmptyNode,currentKeys);
            }
            long currentSize = sumForKeyCombinationUnion(currentKeys);

            Map<String, Object> wouldBeMap = new HashMap<>();
            wouldBeMap.put(keyName, kvf.getKeyValue());
            currentKeys.add(new KeyCombination(wouldBeMap));
            long wouldBeSize = sumForKeyCombinationUnion(currentKeys);


            mostEmptyNode.fillUpBy(wouldBeSize - currentSize);

            fillupQueue.offer(mostEmptyNode);
            keyValueNodeNumber.put(kvf.getKeyValue(), mostEmptyNode);
        }

    }

    private void makeShardingTable(KeyCombination source, List<String> keyNames){
        String keyName;
        if(keyNames.size()>=1){
            keyName = keyNames.get(0);
        }else{
            keyName = null;
        }
        if(keyName==null){
            //exit case
            //lets check which nodes it goes.
            Set<Node> nodesForKeyCombination = new HashSet<>();
            for(Map.Entry<String,Object> kv:source.getKeyValueCombination().entrySet()){
                Node n = keyValueNodeNumberMap.get(kv.getKey()).get(kv.getValue());
                if(n!=null){
                    nodesForKeyCombination.add(n);
                }
            }
            int numberOfIntersectionStorage = source.getKeyValueCombination().size() - nodesForKeyCombination.size();
            Set<Node> nodesToPutBack = new HashSet<>();
            while(numberOfIntersectionStorage>0){
                Node mostEmptyNode = fillupQueue.poll();
                if(!nodesForKeyCombination.contains(mostEmptyNode)){
                    nodesForKeyCombination.add(mostEmptyNode);
                    numberOfIntersectionStorage--;
                }
                nodesToPutBack.add(mostEmptyNode);
            }
            nodesToPutBack.forEach(n -> fillupQueue.offer(n));
            keyCombinationNodeMap.put(source, nodesForKeyCombination);

        }else{
            List<String> restOfKeyNames = new LinkedList<>();
            restOfKeyNames.addAll(keyNames);
            restOfKeyNames.remove(keyName);
            List<KeyValueFrequency> kvfs = keyValueFrequencyMap.get(keyName);

            for(KeyValueFrequency keyValueFrequency:kvfs){
                Object value = keyValueFrequency.getKeyValue();
                Map<String,Object> nextSourceMap = new HashMap<>();
                nextSourceMap.putAll(source.getKeyValueCombination());
                KeyCombination nextSource = new KeyCombination(nextSourceMap);
                nextSource.getKeyValueCombination().put(keyName, value);
                makeShardingTable(nextSource, restOfKeyNames);
            }
        }
    }

    public void shardAllKeys() throws NodeOverflowException {

        for(String key:keyValueFrequencyMap.keySet()){
            shardSingleKey(key);
        }
        List<String> keyNameList = new ArrayList<>();
        keyNameList.addAll(keyValueFrequencyMap.keySet());
        makeShardingTable(new KeyCombination(new HashMap<String, Object>()), keyNameList);
    }

    //This method needs to be generalized
    public void populateFrequencyFromData(BufferedReader data) throws IOException {
        String [] keyNames = {"key1","key2","key3"};
        Map<String,Map<String,Long>> keyValueFrequencyMap = new HashMap<>();
        while(true){
            String line = data.readLine();
            if(line==null){
                break;
            }
            String [] parts = line.split(",");
            String [] keys = new String[3];



            keys[0] = parts[0];
            keys[1] = parts[1];
            keys[2] = parts[2];
//            int key4 = Integer.parseInt(parts[3]);
//            int key5 = Integer.parseInt(parts[4]);
//            int key6 = Integer.parseInt(parts[5]);
//
//            double value1 = Double.parseDouble(parts[6]);
//            double value2 = Double.parseDouble(parts[7]);

            Map<String,Object> keyCombinationMap = new HashMap<>();
            for(int i=0;i<keyNames.length;i++){
                keyCombinationMap.put(keyNames[i],keys[i]);
            }


            KeyCombination keyCombination = new KeyCombination(keyCombinationMap);

            Long count = keyCombinationFrequencyMap.get(keyCombination);
            if(count==null){
                keyCombinationFrequencyMap.put(keyCombination,1L);
            }else{
                keyCombinationFrequencyMap.put(keyCombination,count+1);
            }



            for(int i=0;i<keyNames.length;i++){
                Map<String,Long> frequencyPerValue = keyValueFrequencyMap.get(keyNames[i]);
                if(frequencyPerValue==null){
                    frequencyPerValue = new HashMap<>();
                    keyValueFrequencyMap.put(keyNames[i],frequencyPerValue);
                }

                Long frequency = frequencyPerValue.get(keys[i]);

                if(frequency==null){
                    frequency = 0L;
                }
                frequencyPerValue.put(keys[i],frequency+1);
            }



        }
        for(int i=0;i<keyNames.length;i++) {
            Map<String, Long> frequencyPerValue = keyValueFrequencyMap.get(keyNames[i]);
            List<KeyValueFrequency> freqList = new ArrayList<>();
            this.keyValueFrequencyMap.put(keyNames[i],freqList);
            for(Map.Entry<String,Long> fv: frequencyPerValue.entrySet()){
                freqList.add(new KeyValueFrequency(fv.getKey(),fv.getValue()));
            }
        }
        System.out.println(this.keyValueFrequencyMap);
    }

    public static void main(String [] args) throws Exception {
        /*Sharding sharding = new Sharding(6);
        List<KeyValueFrequency> keyValueFrequencies = new ArrayList<>();
        keyValueFrequencies.add(new KeyValueFrequency(1,300L));
        keyValueFrequencies.add(new KeyValueFrequency(2,100L));
        keyValueFrequencies.add(new KeyValueFrequency(3,20L));
        keyValueFrequencies.add(new KeyValueFrequency(4,15L));
        keyValueFrequencies.add(new KeyValueFrequency(5,13L));
        keyValueFrequencies.add(new KeyValueFrequency(6,13L));
        keyValueFrequencies.add(new KeyValueFrequency(7,14L));
        keyValueFrequencies.add(new KeyValueFrequency(8, 15L));
        keyValueFrequencies.add(new KeyValueFrequency(9,7L));
        keyValueFrequencies.add(new KeyValueFrequency(10, 5L));
        keyValueFrequencies.add(new KeyValueFrequency(11,2L));
        keyValueFrequencies.add(new KeyValueFrequency(12, 1L));


        sharding.keyValueFrequencyMap = new HashMap<>();
        sharding.keyValueFrequencyMap.put("key", keyValueFrequencies);
        sharding.shardSingleKey("key");
        System.out.println(sharding.keyValueNodeNumberMap);

        sharding.keyCombinationFrequencyMap = new HashMap<>();
        HashMap<String, Object> k1Map = new HashMap<>();
        k1Map.put("Country", "US"); k1Map.put("Language", "English");

        HashMap<String, Object> k2Map = new HashMap<>();
        k2Map.put("Country", "US"); k2Map.put("Language", "Hindi");

        HashMap<String, Object> k3Map = new HashMap<>();
        k3Map.put("Country", "India"); k3Map.put("Language", "English");

        HashMap<String, Object> k4Map = new HashMap<>();
        k4Map.put("Country", "India"); k4Map.put("Language", "Hindi");

        sharding.keyCombinationFrequencyMap.put(new KeyCombination(k1Map), 5L);
        sharding.keyCombinationFrequencyMap.put(new KeyCombination(k2Map), 3L);
        sharding.keyCombinationFrequencyMap.put(new KeyCombination(k3Map), 8L);
        sharding.keyCombinationFrequencyMap.put(new KeyCombination(k4Map), 1L);

        HashMap<String, Object> searchKey = new HashMap<>();
        searchKey.put("Language", "English");
        searchKey.put("Country", "US");

        HashMap<String, Object> searchKey2 = new HashMap<>();
        searchKey2.put("Language", "Hindi");

        long sum = sharding.sumForKeyCombinationIntersection(new KeyCombination(searchKey));
        System.out.println(sum);

        sum = sharding.sumForKeyCombinationUnion(new KeyCombination(searchKey));
        System.out.println(sum);

        List<KeyCombination> searchList = new ArrayList<>();
        searchList.add(new KeyCombination(searchKey));
        searchList.add(new KeyCombination(searchKey2));
        sum = sharding.sumForKeyCombinationUnion(searchList);
        System.out.println(sum);*/

        Sharding sharding = new Sharding(20);
        sharding.populateFrequencyFromData(new BufferedReader(new FileReader("sampledata.txt")));
        System.out.println(sharding.keyCombinationFrequencyMap.size());
        System.out.println(sharding.keyCombinationFrequencyMap.entrySet().iterator().next().getValue());
        sharding.shardAllKeys();
        System.out.println(sharding.keyValueNodeNumberMap);
        for(Map.Entry<KeyCombination,Set<Node>> kn:sharding.keyCombinationNodeMap.entrySet()){
            System.out.println(kn.getKey() +" :: "+kn.getValue());
        }

    }

}
