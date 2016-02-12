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
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.CommonUtil.ZKNodeDeleteSignal;
import com.talentica.hungryHippos.utility.PathUtil;

public class JobManager {

	private NodesManager nodesManager;

	private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();

	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);
	private List<Job> jobs = new ArrayList<Job>();

	public void addJobList(List<Job> jobList) {
		this.jobs.addAll(jobList);
	}

	/**
	 * To start the job manager.
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		setBucketToNodeNumberMap();
		LOGGER.info("Initializing nodes manager.");
		(nodesManager = ServerHeartBeat.init()).startup(ZKNodeDeleteSignal.MASTER.name());
		LOGGER.info("SEND TASKS TO NODES");
		sendJobsToNodes();
		LOGGER.info("GET FINISHED SIGNAL FROM NODES");
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
			} /*
				 * else{
				 * LOGGER.info("NODE ID {} FINISHED THE JOBS",node.getNodeId());
				 * }
				 */
		}
		LOGGER.info("ALL NODES FINISHED THE JOBS");
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
		for (Integer nodeId : nodeIdNodeMap.keySet()) {
			if (jobs == null || jobs.isEmpty())
				continue;
			NodeJobsService nodeJobsService = new NodeJobsService(nodeIdNodeMap.get(nodeId), nodesManager);
			nodeJobsService.addJobs(jobs);
			nodeJobsService.createNodeJobService();
			nodeJobsService.scheduleTaskManager();
		}
	}

}
