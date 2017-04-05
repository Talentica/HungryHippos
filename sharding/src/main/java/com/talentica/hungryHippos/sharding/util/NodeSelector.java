/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package com.talentica.hungryHippos.sharding.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;

/**
 * @author pooshans
 *
 */
public class NodeSelector implements Serializable {

  private static final long serialVersionUID = -6600132099728561553L;
  private static Set<Integer> nodeIds = null;
  private static Logger LOGGER = LoggerFactory.getLogger(NodeSelector.class);

  private static void nodeIds() {
    nodeIds = new TreeSet<Integer>();
    ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
    List<com.talentica.hungryhippos.config.cluster.Node> nodes = config.getNode();
    for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
      nodeIds.add(node.getIdentifier());
    }
  }

  public static Set<Integer> selectNodesId(BucketCombination bucketCombination,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      String[] keyOrder) {
    if (nodeIds == null) {
      nodeIds();
    }
    LinkedHashSet<Integer> nodes = new LinkedHashSet<Integer>();
    int numberOfIntersectionStorage = getNumberOfIntersectionPointsByBucketCombinationNode(
        bucketCombination, bucketToNodeNumberMap, keyOrder, nodes);
    selectOtherNodeForIntersectionPoints(nodes, numberOfIntersectionStorage);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.info("BucketCombination {} and Nodes {}", bucketCombination, nodes);
    }
    return nodes;
  }

  private static int getNumberOfIntersectionPointsByBucketCombinationNode(
      BucketCombination bucketCombination,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      String[] keyOrder, LinkedHashSet<Integer> nodes) {
    int numberOfIntersectionStorage = 0;
    for (String key : keyOrder) {
      Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
      Node node = bucketNodeMap.get(bucketCombination.getBucketsCombination().get(key));
      if (!nodes.contains(node.getNodeId())) {
        nodes.add(node.getNodeId());
      } else {
        numberOfIntersectionStorage++;
      }
    }
    return numberOfIntersectionStorage;
  }

  private static void selectOtherNodeForIntersectionPoints(LinkedHashSet<Integer> nodes,
      int numberOfIntersectionStorage) {
    if (numberOfIntersectionStorage > 0) {
      Iterator<Integer> itr = nodeIds.iterator();
      while (itr.hasNext()) {
        int nodeId = itr.next();
        if (!nodes.contains(nodeId)) {
          nodes.add(nodeId);
          numberOfIntersectionStorage--;
        } else {
          continue;
        }
        if (numberOfIntersectionStorage == 0) {
          break;
        }
      }
    }
  }
}
