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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private static LinkedList<Integer> totalNodeIds = null;
  static int maxNodeId;

  /** The logger. */
  private static Logger LOGGER = LoggerFactory.getLogger(NodeSelector.class);

  /**
   * Node ids.
   */
  private static void nodeIds() {
    totalNodeIds = new LinkedList<Integer>();
    ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
    List<com.talentica.hungryhippos.config.cluster.Node> nodes = config.getNode();
    for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
      totalNodeIds.add(node.getIdentifier());
    }
    maxNodeId = totalNodeIds.getLast();
  }

  private static Set<Integer> nodeIds = new LinkedHashSet<Integer>();

  /**
   * Select node ids.
   *
   * @param bucketCombination the bucket combination
   * @param bucketToNodeNumberMap the bucket to node number map
   * @param keyOrder the key order
   * @return the list
   */
  public static Set<Integer> selectNodeIds(BucketCombination bucketCombination,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      String[] keyOrder) {
    if (totalNodeIds == null) {
      nodeIds();
    }
    nodeIds.clear();
    selectPreferredNodeIds(bucketCombination, bucketToNodeNumberMap, keyOrder);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("BucketCombination {} and Nodes {}", bucketCombination, nodeIds);
    }
    return nodeIds;
  }

  /**
   * Select and map index for node.
   *
   * @param bucketCombination the bucket combination
   * @param bucketToNodeNumberMap the bucket to node number map
   * @param keyOrder the key order
   * @param nodes the nodes
   * @param mapIndexesForPreferableNodeIds the map indexes for preferable node ids
   */
  private static void selectPreferredNodeIds(BucketCombination bucketCombination,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
      String[] keyOrder) {
    for (String key : keyOrder) {
      Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
      Node node = bucketNodeMap.get(bucketCombination.getBucketsCombination().get(key));
      if (!nodeIds.contains(node.getNodeId())) {
        nodeIds.add(node.getNodeId());
      } else {
        int nextNodeId = (node.getNodeId() == maxNodeId) ? 0 : (node.getNodeId() + 1);
        while (!totalNodeIds.contains(nextNodeId)) {
          nextNodeId = (nextNodeId == maxNodeId) ? 0 : (nextNodeId + 1);
        }
        nodeIds.add(nextNodeId);
      }
    }
    searchAndInsertRemainingNode(keyOrder);
  }

  private static void searchAndInsertRemainingNode(String[] keyOrder) {
    if (nodeIds.size() < keyOrder.length) {
      Iterator<Integer> iterator = totalNodeIds.iterator();
      while (iterator.hasNext()) {
        int id = iterator.next();
        if (nodeIds.contains(id)) {
          continue;
        } else {
          nodeIds.add(id);
          if (nodeIds.size() == keyOrder.length) {
            break;
          }
        }
      }
    }
  }

}
