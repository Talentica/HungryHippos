package com.talentica.hungryHippos.tools.utils;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryhippos.config.cluster.Node;

import java.util.List;
import java.util.Random;

public class RandomNodePicker {
  
  private static final Random RANDOM = new Random();

  public static Node getRandomNode(){
    List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
    int totalNoOfNodes = nodes.size();
    Node node = nodes.get(RANDOM.nextInt(totalNoOfNodes));
    return node;
  }
}
