/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.testJobs.JobSizeComparator;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
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
	private TaskManager taskManager;
	private NodesManager nodesManager;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeJobsExecutor.class.getName());
	
	public NodeJobsExecutor(Node node,NodesManager nodesManager){
		this.node = node;
		this.nodesManager = nodesManager;
	}
	
	@Override
	public void addJob(Job job) {
		this.jobList.add(job);
	}
	
	@Override
	public void createNodeJobExecutor() throws IOException, InterruptedException, KeeperException, ClassNotFoundException {
		Collections.sort(this.jobList,new JobSizeComparator());
		this.poolCapacity = this.jobList.size();
		TaskManager taskManager = new TaskManager(this.poolCapacity);
		taskManager.setNode(this.node);
		for (Job job : this.jobList) {
			LOGGER.info("JOB ID : {} And RowCount {}",job.getJobId(),job.getDataSize());
			job.status(JobPool.status.POOLED.name());
			taskManager.getJobPoolService().addJob(job);
		}
		this.taskManager = taskManager;
	}

	@Override
	public void scheduleJobNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
		CountDownLatch signal = new CountDownLatch(this.poolCapacity+1);
		while (!taskManager.getJobPoolService().isEmpty()) {
				Job job = taskManager.getJobPoolService().peekJob();
				job.status(JobPool.status.ACTIVE.name());
				boolean flag = sendJobRunnableNotificationToNode(job,signal);
				if(flag){
					taskManager.getJobPoolService().removeJob(job);
				}
		}
		String buildPath = ZKUtils.buildNodePath(node.getNodeId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.START.name();
		nodesManager.createNode(buildPath,signal);
		signal.await();
	}

	@Override
	public void addJob(List<Job> jobs) {
		this.jobList.addAll(jobs);
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
			nodesManager.createNode(buildPath,signal,job);
			flag = true;
		} catch (IOException e) {
			System.out.println("Unable to create node");
		}
		return flag;
	}

	@Override
	public Set<LeafBean> receiveJobSucceedNotificationFromNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException {
		Set<LeafBean> jobLeafs = new HashSet<>();
		String buildpath = ZKUtils.buildNodePath(node.getNodeId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name();
		Set<LeafBean> leafs = ZKUtils.searchTree(buildpath, null);
		for(LeafBean leaf : leafs){
			if(leaf.getPath().contains(CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name())){
				LeafBean jobBean = ZKUtils.getNodeValue(leaf.getPath(),leaf.getPath() + PathUtil.FORWARD_SLASH + leaf.getName(),leaf.getName(), null);
				jobLeafs.add(jobBean);
				
			}
		}
		return jobLeafs;
	}

}
