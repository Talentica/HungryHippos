package com.talentica.hungryHippos;

import java.util.*;

/**
 * Created by debasishc on 14/8/15.
 */
public class Sharding {
    private Map<String,List<KeyValueFrequency>> keyValueFrequencyMap;
    private Map<String,Map<Object, Node>> keyValueNodeNumberMap = new HashMap<>();
    PriorityQueue<Node> fillupQueue = new PriorityQueue<>(new NodeRemainingCapacityComparator());
    private Map<KeyCombination, Long> keyCombinationFrequencyMap;

    public Sharding(int numNodes) {
        for(int i=0;i<numNodes;i++){
            fillupQueue.offer(new Node(10000,i));
        }
    }

    private void shardSingleKey(String keyName) throws NodeOverflowException {
        List<KeyValueFrequency> keyValueFrequencies = keyValueFrequencyMap.get(keyName);
        Map<Object, Node> keyValueNodeNumber =  new HashMap<>();
        keyValueNodeNumberMap.put(keyName, keyValueNodeNumber);
        Collections.sort(keyValueFrequencies);
        for(KeyValueFrequency kvf:keyValueFrequencies){
            Node mostEmptyNode = fillupQueue.poll();
            mostEmptyNode.fillUpBy(kvf.getFrequency());
            fillupQueue.offer(mostEmptyNode);
            keyValueNodeNumber.put(kvf.getKeyValue(), mostEmptyNode);

        }

    }

    public void shardAllKeys() throws NodeOverflowException {
        for(String key:keyValueFrequencyMap.keySet()){
            shardSingleKey(key);
        }
    }

    public static void main(String [] args) throws NodeOverflowException {
        Sharding sharding = new Sharding(6);
        List<KeyValueFrequency> keyValueFrequencies = new ArrayList<>();
        keyValueFrequencies.add(new KeyValueFrequency(1,300L));
        keyValueFrequencies.add(new KeyValueFrequency(2,100L));
        keyValueFrequencies.add(new KeyValueFrequency(3,20L));
        keyValueFrequencies.add(new KeyValueFrequency(4,15L));
        keyValueFrequencies.add(new KeyValueFrequency(5,13L));
        keyValueFrequencies.add(new KeyValueFrequency(6,13L));
        keyValueFrequencies.add(new KeyValueFrequency(7,14L));
        keyValueFrequencies.add(new KeyValueFrequency(8,15L));
        keyValueFrequencies.add(new KeyValueFrequency(9,7L));
        keyValueFrequencies.add(new KeyValueFrequency(10,5L));
        keyValueFrequencies.add(new KeyValueFrequency(11,2L));
        keyValueFrequencies.add(new KeyValueFrequency(12,1L));


        sharding.keyValueFrequencyMap = new HashMap<>();
        sharding.keyValueFrequencyMap.put("key", keyValueFrequencies);
        sharding.shardSingleKey("key");
        System.out.println(sharding.keyValueNodeNumberMap);
    }

}
