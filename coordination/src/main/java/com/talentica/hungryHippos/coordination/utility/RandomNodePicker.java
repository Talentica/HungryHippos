package com.talentica.hungryHippos.coordination.utility;

import java.util.List;
import java.util.Random;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryhippos.config.cluster.Node;

/**
 * 
 * {@code RandomNodePicker} is used to pick a random node from cluster configuration.
 *
 */
public class RandomNodePicker {
  
  private static final Random RANDOM = new Random();

  /**
   * retrieves a random node from cluster configuration file.
   * @return {@link Node}
   */
  public static Node getRandomNode(){
    List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
    int totalNoOfNodes = nodes.size();
    Node node = nodes.get(RANDOM.nextInt(totalNoOfNodes));
    return node;
  }
}
