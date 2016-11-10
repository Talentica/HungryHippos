package com.talentica.hungryHippos;

import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getHHNode;
import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getJobIdNode;
import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getPendingHHNode;
import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getPendingJobIdNode;

import java.util.List;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.PathEnum;
import com.talentica.hungryhippos.config.cluster.Node;

/**
 * {@code JobStatusClientCoordinator} is for client to interact with job status.
 * 
 * @author rajkishoreh
 * @since 4/8/16.
 */
public class JobStatusClientCoordinator {

  private static HungryHippoCurator curator;

  /**
   * Initializes the Job
   * 
   * @param jobUUID
   * @throws HungryHippoException
   */
  public static void initializeJobNodes(String jobUUID) {
    List<Node> nodeList = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
    for (Node node : nodeList) {
      int nodeId = node.getIdentifier();
      String hhNode = getPendingHHNode(nodeId);
      curator = HungryHippoCurator.getInstance();
      try {
        curator.createPersistentNodeIfNotPresent(hhNode);
        String pendingJobIdNode = getPendingJobIdNode(nodeId, jobUUID);
        curator.createPersistentNodeIfNotPresent(pendingJobIdNode);
      } catch (HungryHippoException e) {
        throw new RuntimeException(e.getMessage());
      }

    }
  }

  /**
   * Checks if all the Nodes are completed
   * 
   * @param jobUUID
   * @return
   */
  public static boolean areAllNodesCompleted(String jobUUID) {
    String completedGroup = PathEnum.COMPLETED_JOB_NODES.getPathName();
    List<Node> nodeList = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
    if (nodeList.size() == 0) {
      throw new RuntimeException("Number of nodes in Configuration is zero");
    }
    try {
      curator = HungryHippoCurator.getInstance();
      for (Node node : nodeList) {
        int nodeId = node.getIdentifier();
        String hhNode = getHHNode(completedGroup, jobUUID, nodeId);
        if (!curator.checkExists(hhNode)) {
          return false;
        }
      }
    } catch (HungryHippoException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  /**
   * Checks if any node has failed
   * 
   * @param jobUUID
   * @return
   */
  public static boolean hasAnyNodeFailed(String jobUUID) {
    String failedGroup = PathEnum.FAILED_JOB_NODES.getPathName();
    List<Node> nodeList = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
    if (nodeList.size() == 0) {
      throw new RuntimeException("Number of nodes in Configuration is zero");
    }
    try {
      curator = HungryHippoCurator.getInstance();
      for (Node node : nodeList) {
        int nodeId = node.getIdentifier();
        String hhNode = getHHNode(failedGroup, jobUUID, nodeId);
        if (curator.checkExists(hhNode)) {
          return true;
        }
      }
    } catch (HungryHippoException e) {
      throw new RuntimeException(e);
    }
    return false;
  }

  /**
   * Updates that the Job has completed
   * 
   * @param jobUUID
   */
  public static void updateJobCompleted(String jobUUID) {
    String completedJobsGroup = PathEnum.COMPLETED_JOBS.getPathName();
    String completedJobIdNode = getJobIdNode(completedJobsGroup, jobUUID);
    try {
      curator.createPersistentNodeIfNotPresent(completedJobIdNode);
    } catch (HungryHippoException e) {
      throw new RuntimeException(e.getMessage());
    }

  }

  /**
   * Updates that the Job has failed
   * 
   * @param jobUUID
   */
  public static void updateJobFailed(String jobUUID) {
    String failedJobsGroup = PathEnum.FAILED_JOBS.getPathName();
    String failedJobIdNode = getJobIdNode(failedJobsGroup, jobUUID);
    curator.createPersistentNodeIfNotPresent(failedJobIdNode, "");

  }

}
