package com.talentica.hungryHippos.master.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.ZKNodeName;

public class JobManager {

	private NodesManager nodesManager;

	private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();

	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);
	private List<JobEntity> jobEntities = new ArrayList<JobEntity>();

	public void addJobList(List<Job> jobList) {
		JobEntity jobEntity;
		for (Job job : jobList) {
			jobEntity = new JobEntity();
			jobEntity.setJob(job);
			this.jobEntities.add(jobEntity);
		}
	}

	/**
	 * To start the job manager.
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		setBucketToNodeNumberMap();
		LOGGER.info("Initializing nodes manager.");
		nodesManager = CommonUtil.connectZK();
		LOGGER.info("SEND TASKS TO NODES");
		sendJobsToNodes();
		LOGGER.info("ALL JOBS ARE CREATED ON ZK NODES. PLEASE START ALL NODES");
		sendSignalToAllNodesToStartJobMatrix();
		LOGGER.info("SIGNAL IS SENT TO ALL NODES TO START JOB MATRIX");
		LOGGER.info("START THE KAZOO TO MONITOR THE NODES FOR FINISH");
		CommonUtil.executeScriptCommand("/usr/bin/python","/root/hungryhippos/download-output/"+"start-kazoo-server.py "+CommonUtil.getKazooIp());
		LOGGER.info("KAZOO SERVER IS STARTED");
		getFinishNodeJobsSignal();
		LOGGER.info("\n\n\n\t FINISHED!\n\n\n");
	}

	@SuppressWarnings("unchecked")
	private void setBucketToNodeNumberMap() throws Exception {
		try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
				new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH
						+ Sharding.bucketToNodeNumberMapFile))) {
			this.bucketToNodeNumberMap = (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) inKeyValueNodeNumberMap
					.readObject();
		} catch (IOException | ClassNotFoundException exception) {
			LOGGER.info("Unable to read file");
			throw exception;
		}
	}

	/**
	 * Get finish jobs matrix signal.
	 */
	private void getFinishNodeJobsSignal() {
		Map<Integer, Node> nodeIdNodeMap = getNodeIdNodesMap();
		Iterator<Node> nodesItr = nodeIdNodeMap.values().iterator();
		while (nodesItr.hasNext()) {
			Node node = nodesItr.next();
			if (!getFinishSignal(node.getNodeId(), CommonUtil.ZKJobNodeEnum.FINISH_JOB_MATRIX.name())) {
				continue;
			} 
		}
		LOGGER.info("ALL NODES FINISHED THE JOBS");
	}

	private void sendSignalToAllNodesToStartJobMatrix() throws InterruptedException{
		CountDownLatch signal = new CountDownLatch(1);
			try {
				nodesManager.createPersistentNode(nodesManager.buildAlertPathByName(ZKNodeName.START_JOB_MATRIX), signal);
			} catch (IOException e) {
				LOGGER.info("Unable to send the signal node on zk due to {}",e);
			}
		signal.await();
	
	}
	/**
	 * Get NodeId and Node Map.
	 * 
	 * @return Map<Integer,Node>
	 */
	private Map<Integer, Node> getNodeIdNodesMap() {
		Map<Integer, Node> nodeIdNodeMap = new HashMap<Integer, Node>();
		for (String key : bucketToNodeNumberMap.keySet()) {
			Map<Bucket<KeyValueFrequency>, Node> bucketsToNodeMap = bucketToNodeNumberMap.get(key);
			for (Bucket<KeyValueFrequency> bucket : bucketsToNodeMap.keySet()) {
				Node node = bucketsToNodeMap.get(bucket);
				nodeIdNodeMap.put(node.getNodeId(), node);
			}
		}
		return nodeIdNodeMap;
	}

	/**
	 * Wait for finish signal from node.
	 * 
	 * @param nodeId
	 * @param finishNode
	 * @return boolean
	 */
	private boolean getFinishSignal(Integer nodeId, String finishNode) {
		CountDownLatch signal = new CountDownLatch(1);
		String buildPath = ZKUtils.buildNodePath(nodeId) + PathUtil.FORWARD_SLASH + finishNode;
		try {
			ZKUtils.waitForSignal(buildPath, signal);
			signal.await();
		} catch (KeeperException | InterruptedException e) {
			return false;
		}
		return true;
	}

	/**
	 * Push the jobs to ZK nodes for execution. to run JobRunner on each nodes.
	 * 
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	private void sendJobsToNodes() throws ClassNotFoundException, IOException, InterruptedException, KeeperException {
		Map<Integer, Node> nodeIdNodeMap = getNodeIdNodesMap();
		CommonUtil.generateJobUUID();
		CountDownLatch signal = new CountDownLatch(1);
		for (Integer nodeId : nodeIdNodeMap.keySet()) {
			if (jobEntities == null || jobEntities.isEmpty())
				continue;
			String buildPath = ZKUtils.buildNodePath(nodeId)
					+ PathUtil.FORWARD_SLASH + CommonUtil.getJobUUIdInBase64();
			nodesManager.createPersistentNode(buildPath, signal);
			signal.await();
			NodeJobsService nodeJobsService = new NodeJobsService(nodeIdNodeMap.get(nodeId), nodesManager);
			nodeJobsService.addJobs(jobEntities);
			nodeJobsService.createNodeJobService();
			nodeJobsService.scheduleTaskManager();
		}
		LOGGER.info("Now start the all nodes");
	}

}
