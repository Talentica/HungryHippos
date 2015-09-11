package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;


import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by debasishc on 26/8/15.
 */
public class NodeDataStoreIdCalculator {
    private final Map<String,Map<Object, Node>> keyValueNodeNumberMap;
    private final Node thisNode;
    private final int numberOfDimensions;
    private final Set<Object>[] keyWiseAcceptingValues;
    private final String[] keys;
    private final DynamicMarshal dynamicMarshal;

    public NodeDataStoreIdCalculator(
            Map<String, Map<Object, Node>> keyValueNodeNumberMap, int thisNode, DataDescription dataDescription) {
        this.keyValueNodeNumberMap = keyValueNodeNumberMap;
        this.thisNode = new Node(0, thisNode);
        numberOfDimensions = keyValueNodeNumberMap.size();
        keys =dataDescription.keyOrder();
        keyWiseAcceptingValues = new Set[numberOfDimensions];
        this.dynamicMarshal = new DynamicMarshal(dataDescription);
        for(int i=0;i<keys.length;i++){
            String key=keys[i];
            Set<Object> objs = new HashSet<>();
            for(Map.Entry<Object,Node> e: keyValueNodeNumberMap.get(key).entrySet()){
                if(e.getValue().getNodeId()==thisNode){
                    objs.add(e.getKey());
                }
            }
            keyWiseAcceptingValues[i]=objs;
            System.out.println(keyWiseAcceptingValues[i]);
        }

    }

    public int storeId(ByteBuffer row){
        int fileId=0;
        for(int i=keys.length-1;i>=0;i--){
            fileId<<=1;
            if(keyWiseAcceptingValues[i].contains(dynamicMarshal.readValue(i,row))){
                fileId|=1;
            }

        }
        return fileId;
    }
}
