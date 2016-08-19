package com.talentica.hungryHippos.tools.utils;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryhippos.config.cluster.Node;

import java.util.List;
import java.util.Random;

public class RandomNodePicker {
  
  private static final Random RANDOM = new Random();

  public static Node getRandomNode(){
    List<Node> nodes = CoordinationApplicationContext.getZkClusterConfigCache().getNode();
    int totalNoOfNodes = nodes.size();
    Node node = nodes.remove(RANDOM.nextInt(totalNoOfNodes));
    return node;
  }
}
