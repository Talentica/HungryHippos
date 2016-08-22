package com.talentica.hungryHippos.coordination.utility;

import java.util.List;
import java.util.Random;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryhippos.config.cluster.Node;

public class RandomNodePicker {
  
  private static final Random RANDOM = new Random();

  public static Node getRandomNode(){
    List<Node> nodes = CoordinationApplicationContext.getZkClusterConfigCache().getNode();
    int totalNoOfNodes = nodes.size();
    Node node = nodes.get(RANDOM.nextInt(totalNoOfNodes));
    return node;
  }
}
