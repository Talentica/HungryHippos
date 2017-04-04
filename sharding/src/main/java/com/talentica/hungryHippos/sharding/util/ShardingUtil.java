package com.talentica.hungryHippos.sharding.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;

public class ShardingUtil implements Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = -6600132099728561553L;
  private static Set<Integer> nodesId = null;

  private static void nodesId() {
    nodesId = new TreeSet<Integer>();
    ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
    List<com.talentica.hungryhippos.config.cluster.Node> nodes = config.getNode();
    for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
      nodesId.add(node.getIdentifier());
    }
  }

  public static Set<Integer> getNodesId(BucketCombination bucketCombination,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      String[] keyOrder) {
    if (nodesId == null) {
      nodesId();
    }
    Set<Integer> nodes = new TreeSet<Integer>();
    int noOfIntersection = 0;
    for (String key : keyOrder) {
      Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
      Node node = bucketNodeMap.get(bucketCombination.getBucketsCombination().get(key));
      if (!nodes.contains(node.getNodeId())) {
        nodes.add(node.getNodeId());
      } else {
        noOfIntersection++;
      }
    }
    if (noOfIntersection > 0) {
      Iterator<Integer> itr = nodesId.iterator();
      while (itr.hasNext()) {
        int nodeId = itr.next();
        if (!nodes.contains(nodeId)) {
          nodes.add(nodeId);
          noOfIntersection--;
        } else {
          continue;
        }
        if (noOfIntersection == 0) {
          break;
        }
      }
    }
    return nodes;
  }
}
