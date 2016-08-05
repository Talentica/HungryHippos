package com.talentica.hungryHippos.common.job;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.utility.PathEnum;

/**
 * This class is for having common methods related to Job Status
 * Created by rajkishoreh on 3/8/16.
 */
public class JobStatusCommonOperations {

    /**
     * Returns pendingJobId node
     * @param nodeId
     * @param jobUUID
     * @return
     */
    public static String getPendingJobIdNode(int nodeId, String jobUUID) {
        String hhNode = getPendingHHNode(nodeId);
        String jobIdNode = hhNode + "/" + jobUUID;
        return jobIdNode;
    }

    /**
     * Returns pendingHHNode node
     * @param nodeId
     * @return
     */
    public static String getPendingHHNode(int nodeId) {
        String pendingGroup = PathEnum.PENDING_JOBS.getPathName();
        String groupNode = getGroupNode(pendingGroup);
        String hhNode = groupNode + "/" + nodeId;
        return hhNode;
    }

    /**
     * Returns JobEntityHHNode node
     * @param group
     * @param jobUUID
     * @param jobEntityId
     * @param nodeId
     * @return
     */
    public static String getJobEntityHHNode(String group, String jobUUID, int jobEntityId, int nodeId) {
        String jobEntityNode = getJobEntityNode(group,jobUUID,jobEntityId);
        String hhNode = jobEntityNode + "/" + nodeId;
        return hhNode;
    }

    /**
     * Returns JobEntity node
     * @param group
     * @param jobUUID
     * @param jobEntityId
     * @return
     */
    public static String getJobEntityNode(String group, String jobUUID, int jobEntityId) {
        String jobIdNode = getJobIdNode(group,jobUUID);
        String jobEntityNode = jobIdNode + "/" + jobEntityId;
        return jobEntityNode;
    }

    /**
     * Returns HHNode node of job
     * @param group
     * @param jobUUID
     * @param nodeId
     * @return
     */
    public static String getHHNode(String group, String jobUUID, int nodeId) {
        String jobIdNode = getJobIdNode(group, jobUUID);
        String hhNode = jobIdNode + "/" + nodeId;
        return hhNode;
    }

    /**
     * Returns Job node
     * @param group
     * @param jobUUID
     * @return
     */
    public static String getJobIdNode(String group, String jobUUID) {
        String groupNode = getGroupNode(group);
        String jobIdNode = groupNode + "/" + jobUUID;
        return jobIdNode;
    }

    /**
     * Returns group node
     * @param group
     * @return
     */
    public static String getGroupNode(String group) {
        String jobStatusRootNode = CoordinationApplicationContext.getZkCoordinationConfigCache().
                getZookeeperDefaultConfig().getJobStatusPath();
        String groupNode = jobStatusRootNode + "/" + group;
        return groupNode;
    }


}
