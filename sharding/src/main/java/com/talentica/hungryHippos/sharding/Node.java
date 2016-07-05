package com.talentica.hungryHippos.sharding;

import java.io.Serializable;

/**
 * Created by debasishc on 14/8/15.
 */
public class Node implements Comparable<Node>,Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -6827244296973600470L;
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
            remainingCapacity-=value;
    }

    @Override
    public String toString() {
		return "Node{" + nodeId + " And remainingCapacity "+ remainingCapacity +"}";
    }

	@Override
	public int compareTo(Node o) {
		int ret = 0;
		if(this.nodeId < o.nodeId) ret = -1;
		else if(this.nodeId > o.nodeId) ret = 1;
		else if(this.nodeId == o.nodeId) ret = 0;
		return ret;
	}
}
