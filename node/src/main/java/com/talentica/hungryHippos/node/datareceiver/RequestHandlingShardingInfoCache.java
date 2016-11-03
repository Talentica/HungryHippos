package com.talentica.hungryHippos.node.datareceiver;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code RequestHandlingShardingInfoCache} used for storing information regarding sharding.
 * 
 * @author rajkishoreh
 * @since 28/9/16.
 */
public enum RequestHandlingShardingInfoCache {
  INSTANCE;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(RequestHandlingShardingInfoCache.class);
  private Map<Integer, RequestHandlingShardingInfo> requestHandlingShardingInfoMap;
  private Map<Integer, Set<Integer>> nodeIdToFileIdSet;

  /**
   * creates an instance of RequestHandlingShardingInfoCache.
   */
  RequestHandlingShardingInfoCache() {
    requestHandlingShardingInfoMap = new HashMap<>();
    nodeIdToFileIdSet = new HashMap<>();
  }

  /**
   * retrieves old RequestHandlingShardingInfoCache Object if already instantiated else creates new
   * one.
   * 
   * @param nodeIdClient
   * @param fileId
   * @param hhFilePath
   * @return
   * @throws IOException
   */
  public RequestHandlingShardingInfo get(int nodeIdClient, int fileId, String hhFilePath)
      throws IOException {
    if (nodeIdToFileIdSet.get(nodeIdClient) == null) {
      nodeIdToFileIdSet.put(nodeIdClient, new HashSet<>());
    }
    RequestHandlingShardingInfo requestHandlingShardingInfo =
        requestHandlingShardingInfoMap.get(fileId);
    synchronized (this) {
      if (requestHandlingShardingInfo == null) {
        requestHandlingShardingInfo = new RequestHandlingShardingInfo(fileId, hhFilePath);
        requestHandlingShardingInfoMap.put(fileId, requestHandlingShardingInfo);
        LOGGER.info("Added Cache of FileId : {}", fileId);
      }
      nodeIdToFileIdSet.get(nodeIdClient).add(fileId);
    }
    return requestHandlingShardingInfo;
  }


  /**
   * removes  all the handles for a particular nodeId.
   * 
   * @param nodeIdClient
   * @param fileId
   */
  public void handleRemove(int nodeIdClient, int fileId) {
    if (nodeIdToFileIdSet.get(nodeIdClient) != null) {
      nodeIdToFileIdSet.get(nodeIdClient).remove(fileId);
    }
    LOGGER.info("Removed Link of NodeId : {} for FileId : {}", nodeIdClient, fileId);
    checkAndRemoveShardingInfo(fileId);
  }

  /**
   * remove all the handles for a particular nodeId.
   * 
   * @param nodeIdClient
   */
  public void handleRemoveAll(int nodeIdClient) {
    if (nodeIdToFileIdSet.get(nodeIdClient) != null) {
      nodeIdToFileIdSet.get(nodeIdClient).clear();
    }
    LOGGER.info("Removed Link of NodeId : {} for all FileIds", nodeIdClient);
    for (Integer fileId : requestHandlingShardingInfoMap.keySet()) {
      checkAndRemoveShardingInfo(fileId);
    }
  }

  private void checkAndRemoveShardingInfo(int fileId) {
    boolean deleteSignal = true;
    synchronized (this) {
      for (Map.Entry<Integer, Set<Integer>> entry : nodeIdToFileIdSet.entrySet()) {
        if (entry.getValue().contains(fileId)) {
          LOGGER.info("NodeIdClient :{} is using FileId :{}", entry.getKey(), fileId);
          deleteSignal = false;
          break;
        }
      }
      if (deleteSignal) {
        requestHandlingShardingInfoMap.remove(fileId);
        LOGGER.info("Removed Cache of FileId :{}", fileId);
      }
    }
  }
}
