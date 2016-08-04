package com.talentica.hungryHippos.node.job;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.PathEnum;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.*;

/**
 * Created by rajkishoreh on 4/8/16.
 */
public class JobStatusNodeCoordinator {

    public static List<String> checkNodeJobUUIDs(int nodeId) {
        List<String> listJobUUIDs = new ArrayList<>();
        String pendingHHNode = getPendingHHNode(nodeId);
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            if (manager.checkNodeExists(pendingHHNode)) {
                listJobUUIDs = manager.getChildren(pendingHHNode);
            }
        } catch (FileNotFoundException | JAXBException | KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return listJobUUIDs;
    }

    public static void updateStartedJobEntity(String jobUUID,int jobEntityId, int nodeId) {
        String startedJobEntityGroup = PathEnum.STARTED_JOB_ENTITY.getPathName();
        String jobIdNode = getJobIdNode(startedJobEntityGroup,jobUUID);
        createZKNode(jobIdNode,"");
        String jobEntityNode = getJobEntityNode(startedJobEntityGroup,jobUUID,jobEntityId);
        createZKNode(jobEntityNode,"");
        String jobEntityHHNode = getJobEntityHHNode(startedJobEntityGroup,jobUUID,jobEntityId,nodeId);
        createZKNode(jobEntityHHNode,"");
    }

    public static void updateCompletedJobEntity(String jobUUID,int jobEntityId, int nodeId) {
        String startedJobEntityGroup = PathEnum.STARTED_JOB_ENTITY.getPathName();
        String completedJobEntityGroup = PathEnum.COMPLETED_JOB_ENTITY.getPathName();
        moveJobEntityNode(jobUUID,jobEntityId,nodeId,startedJobEntityGroup,completedJobEntityGroup);
    }

    public static void updateInProgressJob(String jobUUID, int nodeId) {
        String pendingGroup = PathEnum.PENDING_JOBS.getPathName();
        String inProgressGroup = PathEnum.IN_PROGRESS_JOBS.getPathName();
        moveJobNode(jobUUID, nodeId, pendingGroup, inProgressGroup);
    }

    public static void updateNodeJobCompleted(String jobUUID, int nodeId) {
        String inProgressGroup = PathEnum.IN_PROGRESS_JOBS.getPathName();
        String completedGroup = PathEnum.COMPLETED_JOB_NODES.getPathName();
        moveJobNode(jobUUID, nodeId, inProgressGroup, completedGroup);
    }

    public static void updateNodeJobFailed(String jobUUID, int nodeId) {
        String inProgressGroup = PathEnum.IN_PROGRESS_JOBS.getPathName();
        String failedGroup = PathEnum.FAILED_JOB_NODES.getPathName();
        moveJobNode(jobUUID, nodeId, inProgressGroup, failedGroup);
    }
}
