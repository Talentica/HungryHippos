/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import com.talentica.hungryHippos.sharding.Node;

/**
 * @author PooshanS
 *
 */
public class TaskManager {

	private Node node;
	
	private JobPoolService jobPoolService;
	
	private int poolCapacity;
	
	public TaskManager(int poolCapacity){
		this.poolCapacity = poolCapacity;
		jobPoolService = new JobPoolService(this.poolCapacity);
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public JobPoolService getJobPoolService() {
		return jobPoolService;
	}

	public void setJobPoolService(JobPoolService jobPoolService) {
		this.jobPoolService = jobPoolService;
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
		int result = TaskManager.class.hashCode() + node.getNodeId();
		return result;
	}
	
}
