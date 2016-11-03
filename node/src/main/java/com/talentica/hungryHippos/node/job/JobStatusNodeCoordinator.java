package com.talentica.hungryHippos.node.job;


import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getHHNode;
import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getJobEntityHHNode;
import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getJobEntityNode;
import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getJobIdNode;
import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getPendingHHNode;
import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getPendingJobIdNode;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.PathEnum;

/**
 * {@code JobStatusNodeCoordinator} is for Nodes to interact with Job Status Node.
 * 
 * @author rajkishoreh
 * @since 4/8/16.
 */
public class JobStatusNodeCoordinator {

  private static HungryHippoCurator curator;

  static {

    curator = HungryHippoCurator.getInstance();

  }


  /**
   * Returns a list of JobUUIDS assigned to the node
   * 
   * @param nodeId
   * @return
   */
  public static List<String> checkNodeJobUUIDs(int nodeId) {
    String pendingHHNode = getPendingHHNode(nodeId);
    List<String> listJobUUIDs = new ArrayList<>();
    try {
      listJobUUIDs = curator.getChildren(pendingHHNode);
    } catch (HungryHippoException e) {
      // TODO
      e.printStackTrace();
    }
    return listJobUUIDs;
  }

  /**
   * Updates that the JobEntity has started execution
   * 
   * @param jobUUID
   * @param jobEntityId
   * @param nodeId
   */
  public static void updateStartedJobEntity(String jobUUID, int jobEntityId, int nodeId) {
    String startedJobEntityGroup = PathEnum.STARTED_JOB_ENTITY.getPathName();
    String jobIdNode = getJobIdNode(startedJobEntityGroup, jobUUID);
    try {
      curator.createPersistentNodeIfNotPresent(jobIdNode);
      String jobEntityNode = getJobEntityNode(startedJobEntityGroup, jobUUID, jobEntityId);
      curator.createPersistentNodeIfNotPresent(jobEntityNode);
      String jobEntityHHNode =
          getJobEntityHHNode(startedJobEntityGroup, jobUUID, jobEntityId, nodeId);
      curator.createPersistentNodeIfNotPresent(jobIdNode);
    } catch (HungryHippoException e) {
      e.printStackTrace();
    }
  }

  /**
   * Updates that the JobEntity has completed execution
   * 
   * @param jobUUID
   * @param jobEntityId
   * @param nodeId
   */
  public static void updateCompletedJobEntity(String jobUUID, int jobEntityId, int nodeId) {
    String startedJobEntityGroup = PathEnum.STARTED_JOB_ENTITY.getPathName();
    String completedJobEntityGroup = PathEnum.COMPLETED_JOB_ENTITY.getPathName();
    moveJobEntityNode(jobUUID, jobEntityId, nodeId, startedJobEntityGroup, completedJobEntityGroup);
  }

  /**
   * Updates that the Job is in progress
   * 
   * @param jobUUID
   * @param nodeId
   */
  public static void updateInProgressJob(String jobUUID, int nodeId) {
    String pendingGroup = PathEnum.PENDING_JOBS.getPathName();
    String inProgressGroup = PathEnum.IN_PROGRESS_JOBS.getPathName();
    moveJobNode(jobUUID, nodeId, pendingGroup, inProgressGroup);
  }

  /**
   * Updates that the Job for the node has completed
   * 
   * @param jobUUID
   * @param nodeId
   */
  public static void updateNodeJobCompleted(String jobUUID, int nodeId) {
    String inProgressGroup = PathEnum.IN_PROGRESS_JOBS.getPathName();
    String completedGroup = PathEnum.COMPLETED_JOB_NODES.getPathName();
    moveJobNode(jobUUID, nodeId, inProgressGroup, completedGroup);
  }

  /**
   * Updates that the Job for the node has failed
   * 
   * @param jobUUID
   * @param nodeId
   */
  public static void updateNodeJobFailed(String jobUUID, int nodeId) {
    String inProgressGroup = PathEnum.IN_PROGRESS_JOBS.getPathName();
    String failedGroup = PathEnum.FAILED_JOB_NODES.getPathName();
    moveJobNode(jobUUID, nodeId, inProgressGroup, failedGroup);
  }

  /**
   * Moves job from fromGroup to toGroup
   * 
   * @param jobUUID
   * @param nodeId
   * @param fromGroup
   * @param toGroup
   */
  public static void moveJobNode(String jobUUID, int nodeId, String fromGroup, String toGroup) {
    String toJobIdNode = getJobIdNode(toGroup, jobUUID);
    try {
      curator.createPersistentNodeIfNotPresent(toJobIdNode);
      String toHHNode = getHHNode(toGroup, jobUUID, nodeId);
      curator.createPersistentNodeIfNotPresent(toHHNode);
      if (fromGroup.equals(PathEnum.PENDING_JOBS.getPathName())) {
        String fromJobIdNode = getPendingJobIdNode(nodeId, jobUUID);
        curator.delete(fromJobIdNode);
      } else {
        String fromHHNode = getHHNode(fromGroup, jobUUID, nodeId);
        curator.deletePersistentNodeIfExits(fromHHNode);
      }
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }


  }

  /**
   * Moves jobEntity from fromGroup to toGroup
   * 
   * @param jobUUID
   * @param jobEntityId
   * @param nodeId
   * @param fromGroup
   * @param toGroup
   */
  public static void moveJobEntityNode(String jobUUID, int jobEntityId, int nodeId,
      String fromGroup, String toGroup) {


    try {
      String toJobIdNode = getJobIdNode(toGroup, jobUUID);
      curator.createPersistentNodeIfNotPresent(toJobIdNode);
      String toJobEntityNode = getJobEntityHHNode(toGroup, jobUUID, jobEntityId, nodeId);
      curator.createPersistentNodeIfNotPresent(toJobEntityNode);
      String fromJobEntityHHNode = getJobEntityHHNode(fromGroup, jobUUID, jobEntityId, nodeId);
      curator.deletePersistentNodeIfExits(fromJobEntityHHNode);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}
