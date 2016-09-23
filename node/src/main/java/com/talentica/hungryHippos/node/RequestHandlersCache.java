package com.talentica.hungryHippos.node;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rajkishoreh on 22/9/16.
 */
public enum RequestHandlersCache {
  INSTANCE;

  Map<Integer, Map<Integer, RequestHandlingTool>> nodeIdToFileIdToRequestHandlersMap;

  RequestHandlersCache() {
    nodeIdToFileIdToRequestHandlersMap = new HashMap<>();
  }

  public RequestHandlingTool get(int nodeId, int fileId) {
    Map<Integer, RequestHandlingTool> fileIdToRequestHandlerMap =
        nodeIdToFileIdToRequestHandlersMap.get(nodeId);
    if (fileIdToRequestHandlerMap == null) {
      fileIdToRequestHandlerMap = new HashMap<>();
      nodeIdToFileIdToRequestHandlersMap.put(nodeId, fileIdToRequestHandlerMap);
    }
    if (fileIdToRequestHandlerMap.get(fileId) == null) {
      try {
        String path = CoordinationConfigUtil.getZkCoordinationConfigCache()
            .getZookeeperDefaultConfig().getFileidHhfsMapPath();
        String hhFilePath = ZkUtils.getStringDataFromNode(path,fileId+"");
        RequestHandlingTool requestHandlingTool =
            new RequestHandlingTool(fileId, hhFilePath, nodeId + "");
        fileIdToRequestHandlerMap.put(fileId, requestHandlingTool);
      } catch (IOException | InterruptedException | ClassNotFoundException | KeeperException
          | JAXBException e) {
        e.printStackTrace();
      }
    }
    return fileIdToRequestHandlerMap.get(fileId);
  }

    public void removeRequestHandlingTool(int nodeId, int fileId){
        nodeIdToFileIdToRequestHandlersMap.get(nodeId).remove(fileId);
    }

}
