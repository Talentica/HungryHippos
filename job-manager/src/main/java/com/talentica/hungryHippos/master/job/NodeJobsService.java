/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * Scheduling and Managing the Jobs across the nodes on ZK.
 * 
 * @author PooshanS
 *
 */
public class NodeJobsService implements NodesJobsRunnable{

	private List<Job> jobs = new ArrayList<Job>();
	private int poolCapacity;
	private Node node;
	private TaskManager taskManager;
	private NodesManager nodesManager;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeJobsService.class.getName());
	
	public NodeJobsService(Node node,NodesManager nodesManager){
		this.node = node;
		this.nodesManager = nodesManager;
	}
	
	@Override
	public void addJob(Job job) {
		this.jobs.add(job);
	}
	
	@Override
	public void createNodeJobService() throws IOException, InterruptedException, KeeperException, ClassNotFoundException {
		if(this.poolCapacity == 0 && !this.jobs.isEmpty() ) this.poolCapacity = this.jobs.size();
		TaskManager taskManager = new TaskManager(this.poolCapacity);
		taskManager.setNode(this.node);
		for (Job job : this.jobs) {
			job.status(JobPool.status.POOLED.name());
			taskManager.getJobPoolService().addJob(job);
		}
		this.taskManager = taskManager;
	}

	@Override
	public void scheduleTaskManager() throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
		CountDownLatch signal = new CountDownLatch(this.poolCapacity+1);
		while (!taskManager.getJobPoolService().isEmpty()) {
				Job job = taskManager.getJobPoolService().peekJob();
				job.status(JobPool.status.ACTIVE.name());
				boolean flag = sendJobRunnableNotificationToNode(job,signal);
				if(flag){
					taskManager.getJobPoolService().removeJob(job);
				}
		}
		String buildPath = ZKUtils.buildNodePath(node.getNodeId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.START_ROW_COUNT.name();
		nodesManager.createEphemeralNode(buildPath, signal);
		signal.await();
	}

	@Override
	public void addJobs(List<Job> jobs) {
		this.jobs.addAll(jobs);
		this.poolCapacity = jobs.size();
	}

	@Override
	public TaskManager getTaskManager() {
		return this.taskManager;
	}

	@Override
	public boolean sendJobRunnableNotificationToNode(Job job,CountDownLatch signal) throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
		boolean flag = false;
		String buildPath =  ZKUtils.buildNodePath(node.getNodeId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name() + PathUtil.FORWARD_SLASH + ("_job"+job.getJobId());
		try {
			nodesManager.createEphemeralNode(buildPath, signal, job);
			flag = true;
		} catch (IOException e) {
			LOGGER.info("Unable to create node");
		}
		return flag;
	}

	@Override
	public Set<LeafBean> receiveJobSucceedNotificationFromNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
		Set<LeafBean> jobLeafs = new HashSet<>();
		String buildpath = ZKUtils.buildNodePath(node.getNodeId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name();
		Set<LeafBean> leafs = ZKUtils.searchTree(buildpath, null,null);
		for(LeafBean leaf : leafs){
			if(leaf.getPath().contains(CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name())){
				LeafBean jobBean = ZKUtils.getNodeValue(leaf.getPath(),leaf.getPath() + PathUtil.FORWARD_SLASH + leaf.getName(),leaf.getName(), null);
				jobLeafs.add(jobBean);
				
			}
		}
		return jobLeafs;
	}

}
