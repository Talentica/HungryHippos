package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.sharding.Node;
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
            Map<String, Map<Object, Node>> keyValueNodeNumberMap, Node thisNode,
            String [] keyOrder, DynamicMarshal marshal) {
        this.keyValueNodeNumberMap = keyValueNodeNumberMap;
        this.thisNode = thisNode;
        numberOfDimensions = keyValueNodeNumberMap.size();
        keys =keyOrder;
        keyWiseAcceptingValues = new Set[numberOfDimensions];
        this.dynamicMarshal = marshal;
        for(int i=0;i<keys.length;i++){
            String key=keys[i];
            Set<Object> objs = new HashSet<>();
            for(Map.Entry<Object,Node> e: keyValueNodeNumberMap.get(key).entrySet()){
                if(e.getValue().equals(thisNode)){
                    objs.add(e.getKey());
                }
            }
            keyWiseAcceptingValues[i]=objs;
        }
    }

    public int storeId(ByteBuffer row){
        int fileId=0;
        for(int i=0;i<keys.length;i++){
            if(keyWiseAcceptingValues[i].contains(dynamicMarshal.readValue(i,row))){
                fileId++;
            }
            fileId<<=1;
        }
        return fileId;
    }
}
