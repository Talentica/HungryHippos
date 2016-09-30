package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by rajkishoreh on 28/9/16.
 */
public enum RequestHandlingShardingInfoCache {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(RequestHandlingShardingInfoCache.class);
    private Map<Integer,RequestHandlingShardingInfo> requestHandlingShardingInfoMap;
    private Map<Integer,Set<Integer>> nodeIdToFileIdSet;

    RequestHandlingShardingInfoCache() {
        requestHandlingShardingInfoMap = new HashMap<>();
        nodeIdToFileIdSet = new HashMap<>();
    }

    public RequestHandlingShardingInfo get(int nodeIdClient, int fileId, String hhFilePath) throws IOException {
        if(nodeIdToFileIdSet.get(nodeIdClient)==null){
            nodeIdToFileIdSet.put(nodeIdClient,new HashSet<>());
        }
        RequestHandlingShardingInfo requestHandlingShardingInfo = requestHandlingShardingInfoMap.get(fileId);
        synchronized (this){
            if(requestHandlingShardingInfo==null){
                requestHandlingShardingInfo = new RequestHandlingShardingInfo(fileId,hhFilePath);
                requestHandlingShardingInfoMap.put(fileId,requestHandlingShardingInfo);
                LOGGER.info("Added Cache of FileId : {}",fileId);
            }
            nodeIdToFileIdSet.get(nodeIdClient).add(fileId);
        }
        return requestHandlingShardingInfo;
    }


    public void handleRemove(int nodeIdClient, int fileId){
        if(nodeIdToFileIdSet.get(nodeIdClient)!=null) {
            nodeIdToFileIdSet.get(nodeIdClient).remove(fileId);
        }
        LOGGER.info("Removed Link of NodeId : {} for FileId : {}",nodeIdClient,fileId);
        checkAndRemoveShardingInfo(fileId);
    }

    public void handleRemoveAll(int nodeIdClient){
        if(nodeIdToFileIdSet.get(nodeIdClient)!=null){
            nodeIdToFileIdSet.get(nodeIdClient).clear();
        }
        LOGGER.info("Removed Link of NodeId : {} for all FileIds",nodeIdClient);
        for(Integer fileId:requestHandlingShardingInfoMap.keySet()){
            checkAndRemoveShardingInfo(fileId);
        }
    }

    private void checkAndRemoveShardingInfo(int fileId) {
        boolean deleteSignal = true;
        synchronized (this){
            for(Map.Entry<Integer,Set<Integer>> entry:nodeIdToFileIdSet.entrySet()){
                if(entry.getValue().contains(fileId)){
                    LOGGER.info("NodeIdClient :{} is using FileId :{}",entry.getKey(),fileId);
                    deleteSignal = false;
                    break;
                }
            }
            if(deleteSignal){
                requestHandlingShardingInfoMap.remove(fileId);
                LOGGER.info("Removed Cache of FileId :{}",fileId);
            }
        }
    }
}
