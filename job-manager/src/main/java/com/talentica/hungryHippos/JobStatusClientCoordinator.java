package com.talentica.hungryHippos;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.PathEnum;
import com.talentica.hungryhippos.config.cluster.Node;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.util.List;

import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.*;

/**
 * This class is for client to interact with job status
 * Created by rajkishoreh on 4/8/16.
 */
public class JobStatusClientCoordinator {

    /**
     * Initializes the Job
     * @param jobUUID
     */
    public static void initializeJobNodes(String jobUUID) {
        List<Node> nodeList = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        for (Node node : nodeList) {
            int nodeId = node.getIdentifier();
            String hhNode = getPendingHHNode(nodeId);
            ZkUtils.createZKNodeIfNotPresent(hhNode, "");
            String pendingJobIdNode = getPendingJobIdNode(nodeId, jobUUID);
            ZkUtils.createZKNodeIfNotPresent(pendingJobIdNode, "");
        }
    }

    /**
     * Checks if all the Nodes are completed
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
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            for (Node node : nodeList) {
                int nodeId = node.getIdentifier();
                String hhNode = getHHNode(completedGroup, jobUUID, nodeId);
                if (!manager.checkNodeExists(hhNode)) {
                    return false;
                }
            }
        } catch (FileNotFoundException | JAXBException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    /**
     * Checks if any node has failed
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
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            for (Node node : nodeList) {
                int nodeId = node.getIdentifier();
                String hhNode = getHHNode(failedGroup, jobUUID, nodeId);
                if (manager.checkNodeExists(hhNode)) {
                    return true;
                }
            }
        } catch (FileNotFoundException | JAXBException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    /**
     * Updates that the Job has completed
     * @param jobUUID
     */
    public static void updateJobCompleted(String jobUUID) {
        String completedJobsGroup = PathEnum.COMPLETED_JOBS.getPathName();
        String completedJobIdNode = getJobIdNode(completedJobsGroup,jobUUID);
        ZkUtils.createZKNodeIfNotPresent(completedJobIdNode,"");

    }

    /**
     * Updates that the Job has failed
     * @param jobUUID
     */
    public static void updateJobFailed(String jobUUID) {
        String failedJobsGroup = PathEnum.FAILED_JOBS.getPathName();
        String failedJobIdNode = getJobIdNode(failedJobsGroup,jobUUID);
        ZkUtils.createZKNodeIfNotPresent(failedJobIdNode,"");
    }

}
