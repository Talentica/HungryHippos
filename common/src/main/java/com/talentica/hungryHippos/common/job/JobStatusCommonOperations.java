package com.talentica.hungryHippos.common.job;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.PathEnum;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * This class is for having common methods related to Job Status
 * Created by rajkishoreh on 3/8/16.
 */
public class JobStatusCommonOperations {

    /**
     * Moves job from fromGroup to toGroup
     * @param jobUUID
     * @param nodeId
     * @param fromGroup
     * @param toGroup
     */
    public static void moveJobNode(String jobUUID, int nodeId, String fromGroup, String toGroup) {
        String toJobIdNode = getJobIdNode(toGroup, jobUUID);
        createZKNode(toJobIdNode, "");
        String toHHNode = getHHNode(toGroup, jobUUID, nodeId);
        createZKNode(toHHNode, "");
        if (fromGroup.equals(PathEnum.PENDING_JOBS.getPathName())) {
            String fromJobIdNode = getPendingJobIdNode(nodeId, jobUUID);
            deleteZKNode(fromJobIdNode);
        } else {
            String fromHHNode = getHHNode(fromGroup, jobUUID, nodeId);
            deleteZKNode(fromHHNode);
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
        createZKNode(toJobIdNode, "");
        String toJobEntityNode = getJobEntityNode(toGroup, jobUUID, jobEntityId);
        createZKNode(toJobEntityNode, "");
        String fromJobEntityHHNode = getJobEntityHHNode(fromGroup, jobUUID, jobEntityId, nodeId);
        deleteZKNode(fromJobEntityHHNode);
    }

    /**
     * Creates fresh node with data
     * @param node
     * @param data
     */
    public static void createZKNode(String node, Object data) {
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            if (manager.checkNodeExists(node)) {
                deleteZKNode(node);
            }
            CountDownLatch signal = new CountDownLatch(1);
            manager.createPersistentNode(node, signal, data);
            signal.await();
        } catch (IOException | JAXBException | InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes node if it exists
     * @param node
     */
    public static void deleteZKNode(String node) {
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            if (!manager.checkNodeExists(node)) {
                return;
            }
            manager.deleteNode(node);
        } catch (IOException | JAXBException | KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

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
