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

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * Scheduling and Managing the Jobs across the nodes on ZK.
 * 
 * @author PooshanS
 *
 */
public class NodeJobsService implements NodesJobsRunnable {

	private List<JobEntity> jobEntities = new ArrayList<JobEntity>();
	private Node node;
	private TaskManager taskManager;
	private NodesManager nodesManager;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(NodeJobsService.class.getName());

	public NodeJobsService(Node node, NodesManager nodesManager) {
		this.node = node;
		this.nodesManager = nodesManager;
	}

	@Override
	public void addJob(JobEntity jobEntity) {
		this.jobEntities.add(jobEntity);
	}

	@Override
	public void createNodeJobService() throws IOException,
			InterruptedException, KeeperException, ClassNotFoundException {
		TaskManager taskManager = new TaskManager();
		taskManager.setNode(this.node);
		for (JobEntity jobEntity : this.jobEntities) {
			jobEntity.setStatus(JobPool.status.POOLED.name());
			taskManager.getJobPoolService().addJobEntity(jobEntity);
		}
		this.taskManager = taskManager;
	}

	@Override
	public void scheduleTaskManager(String jobUUId) throws InterruptedException,
			KeeperException, ClassNotFoundException, IOException {
		CountDownLatch signal = new CountDownLatch(jobEntities.size());
		while (!taskManager.getJobPoolService().isEmpty()) {
			JobEntity jobEntity = taskManager.getJobPoolService()
					.peekJobEntity();
			jobEntity.setStatus(JobPool.status.ACTIVE.name());
			boolean flag = sendJobRunnableNotificationToNode(jobEntity, signal,jobUUId);
			if (flag) {
				taskManager.getJobPoolService().removeJobEntity(jobEntity);
			}
		}
		signal.await();
	}

	@Override
	public void addJobs(List<JobEntity> jobEntities) {
		this.jobEntities.addAll(jobEntities);
	}

	@Override
	public TaskManager getTaskManager() {
		return this.taskManager;
	}

	@Override
	public boolean sendJobRunnableNotificationToNode(JobEntity jobEntity,
			CountDownLatch signal,String jobUUId) throws InterruptedException,
			KeeperException, ClassNotFoundException, IOException {
		boolean flag = false;
		String buildPath = ZKUtils.buildNodePath(CommonUtil.getJobUUIdInBase64(jobUUId))
				+ PathUtil.SEPARATOR_CHAR + ("_node"+node.getNodeId()) + PathUtil.SEPARATOR_CHAR
				+ CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name() + PathUtil.SEPARATOR_CHAR + ("_job" + jobEntity.getJobId());
		try {
			nodesManager.createPersistentNode(buildPath, signal, jobEntity);
			flag = true;
		} catch (IOException e) {
			LOGGER.info("Unable to create node");
		}
		return flag;
	}

	@Override
	public Set<LeafBean> receiveJobSucceedNotificationFromNode()
			throws InterruptedException, KeeperException,
			ClassNotFoundException, IOException {
		Set<LeafBean> jobLeafs = new HashSet<>();
		String buildpath = ZKUtils.buildNodePath(node.getNodeId())
				+ PathUtil.SEPARATOR_CHAR
				+ CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name();
		Set<LeafBean> leafs = ZKUtils.searchTree(buildpath, null, null);
		for (LeafBean leaf : leafs) {
			if (leaf.getPath().contains(
					CommonUtil.ZKJobNodeEnum.PULL_JOB_NOTIFICATION.name())) {
				LeafBean jobBean = ZKUtils.getNodeValue(
						leaf.getPath(),
						leaf.getPath() + PathUtil.SEPARATOR_CHAR
								+ leaf.getName(), leaf.getName(), null);
				jobLeafs.add(jobBean);

			}
		}
		return jobLeafs;
	}

}
