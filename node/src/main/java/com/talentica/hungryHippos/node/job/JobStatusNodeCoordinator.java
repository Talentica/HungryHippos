package com.talentica.hungryHippos.node.job;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.utility.PathEnum;

import java.util.List;

import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.*;

/**
 * This class is for Nodes to interact with Job Status Node
 * Created by rajkishoreh on 4/8/16.
 */
public class JobStatusNodeCoordinator {

    /**
     * Returns a list of JobUUIDS assigned to the node
     * @param nodeId
     * @return
     */
    public static List<String> checkNodeJobUUIDs(int nodeId) {
        String pendingHHNode = getPendingHHNode(nodeId);
        List<String> listJobUUIDs = ZkUtils.getChildren(pendingHHNode);
        return listJobUUIDs;
    }

    /**
     * Updates that the JobEntity has started execution
     * @param jobUUID
     * @param jobEntityId
     * @param nodeId
     */
    public static void updateStartedJobEntity(String jobUUID,int jobEntityId, int nodeId) {
        String startedJobEntityGroup = PathEnum.STARTED_JOB_ENTITY.getPathName();
        String jobIdNode = getJobIdNode(startedJobEntityGroup,jobUUID);
        ZkUtils.createZKNodeIfNotPresent(jobIdNode,"");
        String jobEntityNode = getJobEntityNode(startedJobEntityGroup,jobUUID,jobEntityId);
        ZkUtils.createZKNodeIfNotPresent(jobEntityNode,"");
        String jobEntityHHNode = getJobEntityHHNode(startedJobEntityGroup,jobUUID,jobEntityId,nodeId);
        ZkUtils.createZKNodeIfNotPresent(jobEntityHHNode,"");
    }

    /**
     * Updates that the JobEntity has completed execution
     * @param jobUUID
     * @param jobEntityId
     * @param nodeId
     */
    public static void updateCompletedJobEntity(String jobUUID,int jobEntityId, int nodeId) {
        String startedJobEntityGroup = PathEnum.STARTED_JOB_ENTITY.getPathName();
        String completedJobEntityGroup = PathEnum.COMPLETED_JOB_ENTITY.getPathName();
        moveJobEntityNode(jobUUID,jobEntityId,nodeId,startedJobEntityGroup,completedJobEntityGroup);
    }

    /**
     * Updates that the Job is in progress
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
     * @param jobUUID
     * @param nodeId
     * @param fromGroup
     * @param toGroup
     */
    public static void moveJobNode(String jobUUID, int nodeId, String fromGroup, String toGroup) {
        String toJobIdNode = getJobIdNode(toGroup, jobUUID);
        ZkUtils.createZKNodeIfNotPresent(toJobIdNode, "");
        String toHHNode = getHHNode(toGroup, jobUUID, nodeId);
        ZkUtils.createZKNodeIfNotPresent(toHHNode, "");
        if (fromGroup.equals(PathEnum.PENDING_JOBS.getPathName())) {
            String fromJobIdNode = getPendingJobIdNode(nodeId, jobUUID);
            ZkUtils.deleteZKNode(fromJobIdNode);
        } else {
            String fromHHNode = getHHNode(fromGroup, jobUUID, nodeId);
            ZkUtils.deleteZKNode(fromHHNode);
        }
    }

    /**
     * Moves jobEntity from fromGroup to toGroup
     * @param jobUUID
     * @param jobEntityId
     * @param nodeId
     * @param fromGroup
     * @param toGroup
     */
    public static void moveJobEntityNode(String jobUUID, int jobEntityId, int nodeId, String fromGroup, String toGroup) {
        String toJobIdNode = getJobIdNode(toGroup, jobUUID);
        ZkUtils.createZKNodeIfNotPresent(toJobIdNode, "");
        String toJobEntityNode = getJobEntityHHNode(toGroup, jobUUID, jobEntityId,nodeId);
        ZkUtils.createZKNodeIfNotPresent(toJobEntityNode, "");
        String fromJobEntityHHNode = getJobEntityHHNode(fromGroup, jobUUID, jobEntityId, nodeId);
        ZkUtils.deleteZKNode(fromJobEntityHHNode);
    }
}
