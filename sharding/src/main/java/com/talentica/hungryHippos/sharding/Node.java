package com.talentica.hungryHippos.sharding;

import java.io.Serializable;

/**
 * Created by debasishc on 14/8/15.
 */
public class Node implements Serializable{
    private int nodeId;
    private long nodeCapacity;
    private long remainingCapacity;

    public Node(long nodeCapacity, int nodeId) {
        this.nodeCapacity = nodeCapacity;
        this.nodeId = nodeId;
        this.remainingCapacity = nodeCapacity;
    }

    public long getNodeCapacity() {
        return nodeCapacity;
    }

    public void setNodeCapacity(long nodeCapacity) {
        this.nodeCapacity = nodeCapacity;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public long getRemainingCapacity() {
        return remainingCapacity;
    }

    public void setRemainingCapacity(long remainingCapacity) {
        this.remainingCapacity = remainingCapacity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        return (nodeId == node.nodeId);

    }

    @Override
    public int hashCode() {
        int result = nodeId;
        return result;
    }

    public void fillUpBy(long value) throws NodeOverflowException{
        if(value>remainingCapacity){
            throw new NodeOverflowException("Node "+nodeId+" ran out of capacity");
        }else{
            remainingCapacity-=value;
        }
    }

    @Override
    public String toString() {
        return "Node{" +
                "nodeCapacity=" + nodeCapacity +
                ", nodeId=" + nodeId +
                ", remainingCapacity=" + remainingCapacity +
                '}';
    }
}
