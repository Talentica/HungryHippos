package com.talentica.hungryHippos.master.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.JobStatusClientCoordinator;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.ShardingTableCache;
import com.talentica.hungryHippos.sharding.ShardingTableFilesName;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.PathUtil;

public class JobManager {

  public static NodesManager nodesManager;

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
  public void start(String jobUUId) throws Exception {
    setBucketToNodeNumberMap();
    LOGGER.info("Initializing nodes manager.");
    LOGGER.info("SEND TASKS TO NODES");
    sendJobsToNodes(jobUUId);
    LOGGER.info("ALL JOBS ARE CREATED ON ZK NODES. PLEASE START ALL NODES");
    JobStatusClientCoordinator.initializeJobNodes(jobUUId);
    LOGGER.info("SIGNAL IS SENT TO ALL NODES TO START JOB MATRIX");
    boolean areNodesCompleted = checkNodesStatusSignal();
    if (areNodesCompleted) {
      JobStatusClientCoordinator.updateJobCompleted(jobUUId);
    } else {
      JobStatusClientCoordinator.updateJobFailed(jobUUId);
    }
    LOGGER.info("\n\n\n\t FINISHED!\n\n\n");
  }

  @SuppressWarnings("unchecked")
  private void setBucketToNodeNumberMap() throws Exception {
    // TODO figuring out the path of sharding table on Zk.
    ShardingTableCache shardingTableCache = ShardingTableCache.newInstance();
    // TODO change the path parameter of the getShardingTableFromCache method.
    bucketToNodeNumberMap = (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) shardingTableCache
        .getShardingTableFromCache(ShardingTableFilesName.BUCKET_TO_NODE_NUMBER_MAP_FILE.getName(),
            "<sharding_table_path>");
  }

  /**
   * Get finish jobs matrix signal.
   */
  private void getFinishNodeJobsSignal(String nodeName) {
    Map<Integer, Node> nodeIdNodeMap = getNodeIdNodesMap();
    Iterator<Node> nodesItr = nodeIdNodeMap.values().iterator();
    while (nodesItr.hasNext()) {
      Node node = nodesItr.next();
      if (!getSignalFromZk(node.getNodeId(), nodeName)) {
        continue;
      }
    }
    LOGGER.info("ALL NODES FINISHED THE JOBS");
  }

  /**
   * Waits and checks the outcome of the Nodes
   * 
   * @return
   */
  private boolean checkNodesStatusSignal() {
    boolean areNodesCompleted = false;
    boolean hasAnyNodeFailed = false;
    while (!(areNodesCompleted || hasAnyNodeFailed)) {
      areNodesCompleted =
          JobStatusClientCoordinator.areAllNodesCompleted(ZkSignalListener.jobuuidInBase64);
      hasAnyNodeFailed =
          JobStatusClientCoordinator.hasAnyNodeFailed(ZkSignalListener.jobuuidInBase64);
    }
    LOGGER.info("ALL NODES FINISHED THE JOBS");
    return areNodesCompleted;

  }

  private void sendSignalEndJobMatrix() throws InterruptedException {
    CountDownLatch signal = new CountDownLatch(1);
    String buildFinishPath = ZkUtils.buildNodePath(ZkSignalListener.jobuuidInBase64)
        + PathUtil.SEPARATOR_CHAR + CommonUtil.ZKJobNodeEnum.END_JOB_MATRIX.getZKJobNode();
    try {
      nodesManager.createPersistentNode(buildFinishPath, signal);
    } catch (IOException e) {
      LOGGER.info("Unable to send the signal node on zk due to {}", e);
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
  private boolean getSignalFromZk(Integer nodeId, String finishNode) {
    CountDownLatch signal = new CountDownLatch(1);

    String buildFinishPath = ZkUtils.buildNodePath(ZkSignalListener.jobuuidInBase64)
        + PathUtil.SEPARATOR_CHAR + ("_node" + nodeId) + PathUtil.SEPARATOR_CHAR + finishNode;

    try {
      ZkUtils.waitForSignal(buildFinishPath, signal);
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
  private void sendJobsToNodes(String jobUUId)
      throws ClassNotFoundException, IOException, InterruptedException, KeeperException {
    Map<Integer, Node> nodeIdNodeMap = getNodeIdNodesMap();

    for (Integer nodeId : nodeIdNodeMap.keySet()) {
      if (jobEntities == null || jobEntities.isEmpty())
        continue;
      NodeJobsService nodeJobsService =
          new NodeJobsService(nodeIdNodeMap.get(nodeId), nodesManager);
      nodeJobsService.addJobs(jobEntities);
      nodeJobsService.createNodeJobService();
      nodeJobsService.scheduleTaskManager(jobUUId);
    }
    LOGGER.info("Now start the all nodes");
  }

}
