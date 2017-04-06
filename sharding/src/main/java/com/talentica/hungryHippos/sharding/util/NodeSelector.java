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
import java.util.ArrayList;
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
 * The Class NodeSelector.
 *
 * @author pooshans
 */
public class NodeSelector implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = -6600132099728561553L;
  
  /** The node ids. */
  private static Set<Integer> nodeIds = null;
  
  /** The logger. */
  private static Logger LOGGER = LoggerFactory.getLogger(NodeSelector.class);

  /**
   * Node ids.
   */
  private static void nodeIds() {
    nodeIds = new TreeSet<Integer>();
    ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
    List<com.talentica.hungryhippos.config.cluster.Node> nodes = config.getNode();
    for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
      nodeIds.add(node.getIdentifier());
    }
  }

 
  /**
   * Select node ids.
   *
   * @param bucketCombination the bucket combination
   * @param bucketToNodeNumberMap the bucket to node number map
   * @param keyOrder the key order
   * @return the sets the
   */
  public static Set<Integer> selectNodeIds(BucketCombination bucketCombination,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      String[] keyOrder) {
    if (nodeIds == null) {
      nodeIds();
    }
    LinkedHashSet<Integer> nodeIds = new LinkedHashSet<Integer>();
    List<Integer> nodes = new ArrayList<Integer>();
    List<Integer> indexToInsertPrefferedNodeId = getNumberOfIntersectionStoragePointsByBucketCombinationNode(
        bucketCombination, bucketToNodeNumberMap, keyOrder, nodes);
    preferDifferentNodeForIntersectionStoragePoints(nodes, indexToInsertPrefferedNodeId);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("BucketCombination {} and Nodes {}", bucketCombination, nodes);
    }
    nodes.forEach(id -> nodeIds.add(id));
    return nodeIds;
  }

  /**
   * Gets the number of intersection storage points by bucket combination node.
   *
   * @param bucketCombination the bucket combination
   * @param bucketToNodeNumberMap the bucket to node number map
   * @param keyOrder the key order
   * @param nodes the nodes
   * @return the number of intersection storage points by bucket combination node
   */
  private static List<Integer> getNumberOfIntersectionStoragePointsByBucketCombinationNode(
      BucketCombination bucketCombination,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      String[] keyOrder, List<Integer> nodes) {
    List<Integer> indexToInsertPrefferedNodeId = new ArrayList<>();
    int index = 0;
    for (String key : keyOrder) {
      Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
      Node node = bucketNodeMap.get(bucketCombination.getBucketsCombination().get(key));
      if (!nodes.contains(node.getNodeId())) {
        nodes.add(node.getNodeId());
      } else {
        indexToInsertPrefferedNodeId.add(index);
      }
      index++;
    }
    return indexToInsertPrefferedNodeId;
  }

  /**
   * Prefer different node for intersection storage points.
   *
   * @param nodes the nodes
   * @param indexToInsertPrefferedNodeId the index to insert preffered node id
   */
  private static void preferDifferentNodeForIntersectionStoragePoints(List<Integer> nodes,
      List<Integer> indexToInsertPrefferedNodeId) {
    if (indexToInsertPrefferedNodeId.size() > 0) {
      Iterator<Integer> itr = nodeIds.iterator();
      int index = 0;
      while (itr.hasNext()) {
        int nodeId = itr.next();
        if (!nodes.contains(nodeId)) {
          nodes.add(indexToInsertPrefferedNodeId.get(index),nodeId);
        } else {
          continue;
        }
        if (index == indexToInsertPrefferedNodeId.size() -1) {
          break;
        }
        index++;
      }
    }
  }
}
