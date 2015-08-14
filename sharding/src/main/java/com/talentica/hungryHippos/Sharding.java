package com.talentica.hungryHippos;

import java.util.*;

/**
 * Created by debasishc on 14/8/15.
 */
public class Sharding {
    private Map<String,List<KeyValueFrequency>> keyValueFrequencyMap;
    private Map<String,Map<Object, Node>> keyValueNodeNumberMap = new HashMap<>();
    PriorityQueue<Node> fillupQueue = new PriorityQueue<>(new NodeRemainingCapacityComparator());



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


}
