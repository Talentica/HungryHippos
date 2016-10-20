package com.talentica.hungryHippos.node.datareceiver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;

/**
 * Created by rajkishoreh on 22/9/16.
 */
public enum RequestHandlersCache {
  INSTANCE;
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestHandlersCache.class);
  Map<Integer, Map<Integer, RequestHandlingTool>> nodeIdToFileIdToRequestHandlersMap;

  RequestHandlersCache() {
    nodeIdToFileIdToRequestHandlersMap = new HashMap<>();
  }

  public RequestHandlingTool get(int nodeIdClient, int fileId) {
    Map<Integer, RequestHandlingTool> fileIdToRequestHandlerMap =
        nodeIdToFileIdToRequestHandlersMap.get(nodeIdClient);
    if (fileIdToRequestHandlerMap == null) {
      fileIdToRequestHandlerMap = new HashMap<>();
      nodeIdToFileIdToRequestHandlersMap.put(nodeIdClient, fileIdToRequestHandlerMap);
    }
    if (fileIdToRequestHandlerMap.get(fileId) == null) {
      try {
        String path = CoordinationConfigUtil.getZkCoordinationConfigCache()
            .getZookeeperDefaultConfig().getFileidHhfsMapPath();
        String hhFilePath = HungryHippoCurator.getAlreadyInstantiated()
            .getZnodeData(path + HungryHippoCurator.ZK_PATH_SEPERATOR + fileId + "");
        LOGGER.info("NodeIdClient :{} FileId :{} FilePath :{}", nodeIdClient, fileId, hhFilePath);
        RequestHandlingTool requestHandlingTool =
            new RequestHandlingTool(fileId, hhFilePath, nodeIdClient);
        fileIdToRequestHandlerMap.put(fileId, requestHandlingTool);
      } catch (IOException | InterruptedException | ClassNotFoundException | KeeperException
          | JAXBException | HungryHippoException e) {
        LOGGER.error(e.toString());
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    return fileIdToRequestHandlerMap.get(fileId);
  }

  public void removeRequestHandlingTool(int nodeId, int fileId) {
    if (nodeIdToFileIdToRequestHandlersMap.get(nodeId) != null) {
      nodeIdToFileIdToRequestHandlersMap.get(nodeId).remove(fileId);
    }
    LOGGER.info("Removed link of NodeId :{} from FileId :{}", nodeId, fileId);
  }

  public void removeAllRequestHandlingTool(int nodeId) {
    if (nodeIdToFileIdToRequestHandlersMap.get(nodeId) != null) {
      for (Map.Entry<Integer, RequestHandlingTool> toolEntry : nodeIdToFileIdToRequestHandlersMap
          .get(nodeId).entrySet()) {
        toolEntry.getValue().close();
      }
    }
    nodeIdToFileIdToRequestHandlersMap.remove(nodeId);
    LOGGER.info("Removed link of NodeId : {} for all FileIds", nodeId);
  }

}