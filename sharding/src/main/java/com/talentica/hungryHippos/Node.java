package com.talentica.hungryHippos;

/**
 * Created by debasishc on 14/8/15.
 */
public class Node {
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

        if (nodeId != node.nodeId) return false;
        if (nodeCapacity != node.nodeCapacity) return false;
        return remainingCapacity == node.remainingCapacity;

    }

    @Override
    public int hashCode() {
        int result = nodeId;
        result = 31 * result + (int) (nodeCapacity ^ (nodeCapacity >>> 32));
        result = 31 * result + (int) (remainingCapacity ^ (remainingCapacity >>> 32));
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
