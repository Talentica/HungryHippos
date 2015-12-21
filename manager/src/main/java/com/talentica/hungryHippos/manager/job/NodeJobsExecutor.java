/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.zookeeper.LeafBean;
import com.talentica.hungryHippos.utility.zookeeper.ZKUtils;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

/**
 * @author PooshanS
 *
 */
public class NodeJobsExecutor implements NodesJobsRunnable{

	private List<Job> jobList = new ArrayList<>();
	private int poolCapacity;
	private Node node;
	private NodeJob nodeJob;
	private NodesManager nodesManager;
	
	public NodeJobsExecutor(Node node,NodesManager nodesManager){
		this.node = node;
		this.nodesManager = nodesManager;
	}
	
	@Override
	public void addJob(Job job) {
		this.jobList.add(job);
	}
	
	@Override
	public void createNodeJobExecutor() throws IOException {
		this.poolCapacity = jobList.size();
		NodeJob nodeJob = new NodeJob(this.poolCapacity);
		nodeJob.setNode(this.node);
		
		createZKNode("_node"+node.getNodeId(),CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name());
		createZKNode("_node"+node.getNodeId(),CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name());
		
		for (Job job : this.jobList) {
			job.status(JobPool.status.POOLED.name());
			nodeJob.getJobPoolExecutor().addJob(job);
		}
		this.nodeJob = nodeJob;
	}

	@Override
	public void scheduleJobNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
		Set<LeafBean> jobLeafNotified;
		while (!nodeJob.getJobPoolExecutor().isEmpty()) {
			Job job = nodeJob.getJobPoolExecutor().pollJob();
			job.status(JobPool.status.ACTIVE.name());
			sendJobRunnableNotificationToNode(job);
			jobLeafNotified = receiveJobSucceedNotificationFromNode();
			if(jobLeafNotified != null){
				for(LeafBean jobLeaf : jobLeafNotified){
					Job jobNotified = (Job) ZKUtils.deserialize(jobLeaf.getValue());
					nodeJob.getJobPoolExecutor().removeJob(jobNotified);
					nodesManager.deleteNode(jobLeaf.getPath());
				}
			}
		}
	}

	@Override
	public void addJob(List<Job> jobs) {
		this.jobList.addAll(jobs);
		this.poolCapacity = jobs.size();
	}

	@Override
	public NodeJob getNodeJob() {
		return this.nodeJob;
	}

	@Override
	public void sendJobRunnableNotificationToNode(Job job) throws InterruptedException, KeeperException {
		String buildPath = null ;
		Set<LeafBean> leafs = ZKUtils.searchTree(CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name(), null);
		for(LeafBean leaf : leafs){
			if(leaf.getName().equals(CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name())){
				buildPath = nodesManager.buildPathChieldNode(CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name(),String.valueOf(job.getJobId()));
				break;
			}
		}
		try {
			nodesManager.createNode(buildPath,job);
		} catch (IOException e) {
			System.out.println("Unable to create node");
		}
	}

	@Override
	public Set<LeafBean> receiveJobSucceedNotificationFromNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
		Set<LeafBean> jobLeafs = null;
		Set<LeafBean> leafs = ZKUtils.searchTree(CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name(), null);
		for(LeafBean leaf : leafs){
			if(leaf.getName().equals(CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name()) && leaf.getPath().contains("_node"+node.getNodeId())){
				jobLeafs = ZKUtils.searchTree(leaf.getName(), null);
				break;
			}
		}
		return jobLeafs;
	}
	
	
	private void createZKNode(String parentNode,String chieldNode) throws IOException{
		String buildPath = nodesManager.buildPathChieldNode(parentNode,chieldNode);
		nodesManager.createNode(buildPath);
	}
	

}
