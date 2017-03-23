/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.sharding;

import java.io.Serializable;

import com.talentica.hungryHippos.coordination.annotations.ZkTransient;

/**
 * {@code Node} used for keeping track of each Node related details.
 * 
 * @author debasishc
 * @since 14/8/15.
 */
public class Node implements Comparable<Node>, Serializable {
  /**
   * 
   */
  @ZkTransient
  private static final long serialVersionUID = -6827244296973600470L;
  private int nodeId;
  private long nodeCapacity;
  private long remainingCapacity;
  private float bucketCountWeight;
  private int bucketCount;

  /**
   * creates an empty instance of Node.
   */
  public Node() {}

  /**
   * creates a new instance of Node with specified capacity.
   * 
   * @param nodeCapacity
   * @param nodeId
   */
  public Node(long nodeCapacity, int nodeId, float bucketCountWeight) {
    this.nodeCapacity = nodeCapacity;
    this.nodeId = nodeId;
    this.remainingCapacity = nodeCapacity;
    this.bucketCount = 0;
    this.bucketCountWeight = bucketCountWeight;
  }

  /**
   * retrieves the node capacity.
   * @return
   */
  public long getNodeCapacity() {
    return nodeCapacity;
  }

  /**
   * sets the nodeCapacity.
   * @param nodeCapacity
   */
  public void setNodeCapacity(long nodeCapacity) {
    this.nodeCapacity = nodeCapacity;
  }

  /**
   * retrieves the Node Id.
   * @return
   */
  public int getNodeId() {
    return nodeId;
  }

  /**
   * sets the nodeId.
   * @param nodeId
   */
  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  /**
   * retrieves the remaining capacity of this node.
   * @return
   */
  public long getRemainingCapacity() {
    return (long)((1-bucketCountWeight)*remainingCapacity-bucketCountWeight*nodeCapacity*bucketCount);
  }

  /**
   * sets the remaining capacity of this node.
   * @param remainingCapacity
   */
  public void setRemainingCapacity(long remainingCapacity) {
    this.remainingCapacity = remainingCapacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Node node = (Node) o;

    return (nodeId == node.nodeId);

  }

  @Override
  public int hashCode() {
    int result = nodeId;
    return result;
  }

  public void fillUpBy(long value) throws NodeOverflowException {
    remainingCapacity -= value;
    bucketCount++;
  }

  @Override
  public String toString() {
    return "Node{" + nodeId + "}";
  }

  @Override
  public int compareTo(Node o) {
    int ret = 0;
    if (this.nodeId < o.nodeId)
      ret = -1;
    else if (this.nodeId > o.nodeId)
      ret = 1;
    else if (this.nodeId == o.nodeId)
      ret = 0;
    return ret;
  }

  public int getBucketCount() {
    return bucketCount;
  }
}
