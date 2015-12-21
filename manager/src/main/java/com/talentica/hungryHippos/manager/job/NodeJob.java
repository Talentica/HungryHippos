/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import com.talentica.hungryHippos.sharding.Node;

/**
 * @author PooshanS
 *
 */
public class NodeJob {

	private Node node;
	
	private JobPoolExecutor jobPoolExecutor;
	
	private int poolCapacity;
	
	public NodeJob(int poolCapacity){
		this.poolCapacity = poolCapacity;
		jobPoolExecutor = new JobPoolExecutor(this.poolCapacity);
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public JobPoolExecutor getJobPoolExecutor() {
		return jobPoolExecutor;
	}

	public void setJobPoolExecutor(JobPoolExecutor jobPoolExecutor) {
		this.jobPoolExecutor = jobPoolExecutor;
	}
	
	
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Node node = (Node) o;
		return (node.getNodeId() == node.getNodeId());

	}

	@Override
	public int hashCode() {
		int result = NodeJob.class.hashCode() + node.getNodeId();
		return result;
	}
	
}
