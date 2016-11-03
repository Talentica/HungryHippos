package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.context.DataPublisherApplicationContext;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code NodeConnectionPool} used for establishing  connection related to Nodes.
 * @author rajkishoreh
 * @since 22/9/16.
 */
public enum NodeConnectionPool {
  INSTANCE;
  private final Logger logger = LoggerFactory.getLogger(NodeConnectionPool.class);
  private final Map<Integer, Socket> nodeConnectionMap;
  private final int ownNodeId;
  private List<Node> nodes;

  NodeConnectionPool() {
    logger.info("Establishing Connections");
    nodeConnectionMap = new HashMap<>();
    nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
    ownNodeId = NodeInfo.INSTANCE.getIdentifier();
    for (Node node : nodes) {
      createConnection(node);
    }
  }

  private void createConnection(Node node) {
    int nodeId = node.getIdentifier();
    if (nodeId != ownNodeId) {
      String nodeIp = node.getIp();
      int port = Integer.parseInt(node.getPort());
      logger.info("Connecting to {}", nodeIp);
      int noOfAttemptsToConnectToNode =
          DataPublisherApplicationContext.getNoOfAttemptsToConnectToNode();
      while (true) {
        try {
          Socket socket = new Socket(nodeIp, port);
          nodeConnectionMap.put(nodeId, socket);
          break;
        } catch (IOException e) {
          if (noOfAttemptsToConnectToNode == 0) {
            throw new RuntimeException("Couldn't connect to server " + nodeIp);
          }
          logger.error(e.toString());
          logger.info("Retrying connection after 1 second");
          try {
            Thread.sleep(DataPublisherApplicationContext.getServersConnectRetryIntervalInMs());
          } catch (InterruptedException e1) {
            logger.error(e1.toString());
          }
          noOfAttemptsToConnectToNode--;
        }
      }
      logger.info("Connected to {}", nodeIp);

    }
    logger.info("Established Connections");
  }

  /**
   * retrieves the connection map.
   * @return
   */
  public Map<Integer, Socket> getNodeConnectionMap() {
    return nodeConnectionMap;
  }

  /**
   * tries to reconnect to the node {@value nodeId}.
   * @param nodeId
   */
  public void reConnect(int nodeId) {
    Node nodeToConnect = null;
    for (Node node : nodes) {
      if (node.getIdentifier() != ownNodeId && nodeId == node.getIdentifier()) {
        createConnection(node);
        nodeToConnect = node;
        break;
      }
    }
    if (nodeToConnect == null) {
      throw new RuntimeException("Invalid nodeId passed");
    }
  }


  public void closeAllConnections() {
    for (Map.Entry<Integer, Socket> nodeConnection : nodeConnectionMap.entrySet()) {
      if (nodeConnection.getValue() != null) {
        try {
          nodeConnection.getValue().close();
        } catch (IOException e) {
          logger.error("Error while closing connection to Node {}", nodeConnection.getKey());
          logger.error(e.toString());
        }
      }
    }
  }

}
