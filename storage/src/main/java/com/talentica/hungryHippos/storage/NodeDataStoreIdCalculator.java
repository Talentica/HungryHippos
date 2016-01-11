package com.talentica.hungryHippos.storage;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 26/8/15.
 */
public class NodeDataStoreIdCalculator implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -4962284637100465382L;
	private final Map<String,Map<Object, Node>> keyValueNodeNumberMap;
    private final Node thisNode;
    private final int numberOfDimensions;
    private final Set<Object>[] keyWiseAcceptingValues;
    private final String[] keys;
    private final DynamicMarshal dynamicMarshal;
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeDataStoreIdCalculator.class.getName());
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
            LOGGER.info("keyWiseAcceptingValues :: "+keyWiseAcceptingValues[i]);
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
