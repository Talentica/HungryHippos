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
 * Created by rajkishoreh on 3/8/16.
 */
public class JobStatusCommonOperations {

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

    public static void moveJobEntityNode(String jobUUID, int jobEntityId, int nodeId, String fromGroup, String toGroup) {
        String toJobIdNode = getJobIdNode(toGroup, jobUUID);
        createZKNode(toJobIdNode, "");
        String toJobEntityNode = getJobEntityNode(toGroup, jobUUID, jobEntityId);
        createZKNode(toJobEntityNode, "");
        String fromJobEntityHHNode = getJobEntityHHNode(fromGroup, jobUUID, jobEntityId, nodeId);
        deleteZKNode(fromJobEntityHHNode);
    }

    public static void createZKNode(String node, Object data) {
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            if (manager.checkNodeExists(node)) {
                return;
            }
            CountDownLatch signal = new CountDownLatch(1);
            manager.createPersistentNode(node, signal, data);
            signal.await();
        } catch (IOException | JAXBException | InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }

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

    public static String getPendingJobIdNode(int nodeId, String jobUUID) {
        String hhNode = getPendingHHNode(nodeId);
        String jobIdNode = hhNode + "/" + jobUUID;
        return jobIdNode;
    }

    public static String getPendingHHNode(int nodeId) {
        String pendingGroup = PathEnum.PENDING_JOBS.getPathName();
        String groupNode = getGroupNode(pendingGroup);
        String hhNode = groupNode + "/" + nodeId;
        return hhNode;
    }

    public static String getJobEntityHHNode(String group, String jobUUID, int jobEntityId, int nodeId) {
        String jobEntityNode = getJobEntityNode(group,jobUUID,jobEntityId);
        String hhNode = jobEntityNode + "/" + nodeId;
        return hhNode;
    }

    public static String getJobEntityNode(String group, String jobUUID, int jobEntityId) {
        String jobIdNode = getJobIdNode(group,jobUUID);
        String jobEntityNode = jobIdNode + "/" + jobEntityId;
        return jobEntityNode;
    }

    public static String getHHNode(String group, String jobUUID, int nodeId) {
        String jobIdNode = getJobIdNode(group, jobUUID);
        String hhNode = jobIdNode + "/" + nodeId;
        return hhNode;
    }

    public static String getJobIdNode(String group, String jobUUID) {
        String groupNode = getGroupNode(group);
        String jobIdNode = groupNode + "/" + jobUUID;
        return jobIdNode;
    }

    public static String getGroupNode(String group) {
        String jobStatusRootNode = CoordinationApplicationContext.getZkCoordinationConfigCache().
                getZookeeperDefaultConfig().getJobStatusPath();
        String groupNode = jobStatusRootNode + "/" + group;
        return groupNode;
    }


}
