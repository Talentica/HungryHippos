/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import com.talentica.hungryHippos.sharding.Node;

/**
 * 
 * It manages the the jobs for each nodes on the Priority Queue of Jobs.
 * 
 * @author PooshanS
 *
 */
public class TaskManager {

	private Node node;
	
	private JobPoolService jobPoolService;
	
	
	public TaskManager(){
		jobPoolService = new JobPoolService();
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
