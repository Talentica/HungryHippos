package com.talentica.hungryHippos;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.JobEntity;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.*;

/**
 * Created by rajkishoreh on 2/8/16.
 */
public class JobConfigPublisher {

    public static final Logger LOGGER = LoggerFactory.getLogger(JobConfigPublisher.class);


    /**
     * Publishes Job configurations to the zookeeper
     *
     * @param jobUUID
     * @param jobMatrixClass
     * @param inputHHPath
     * @param outputHHPath
     * @return
     */
    public static boolean publish(String jobUUID, String jobMatrixClass, String inputHHPath, String outputHHPath) {
        try {
            String jobNode = getJobNode(jobUUID);
            createZKNode(jobNode, "");
            String jobClassNode = getJobClassNode(jobNode);
            createZKNode(jobClassNode, jobMatrixClass);
            String jobInputNode = getJobInputNode(jobNode);
            createZKNode(jobInputNode, inputHHPath);
            String jobOutputNode = getJobOutputNode(jobNode);
            createZKNode(jobOutputNode, outputHHPath);
            validateConfigNodes(jobUUID, jobMatrixClass, inputHHPath, outputHHPath);
            return true;
        } catch (Exception e) {
            LOGGER.error(e.toString());
            return false;
        }
    }


    private static void validateConfigNodes(String jobUUID, String jobMatrixClass, String inputHHPath, String outputHHPath) {
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            String jobNode = getJobNode(jobUUID);
            boolean jobNodeExists = manager.checkNodeExists(jobNode);
            if (!jobNodeExists) {
                throw new RuntimeException(jobNode + " not created");
            }
            String jobClassNode = getJobClassNode(jobNode);
            compareWithNodeData(jobClassNode, jobMatrixClass);
            String jobInputNode = getJobInputNode(jobNode);
            compareWithNodeData(jobInputNode, inputHHPath);
            String jobOutputNode = getJobOutputNode(jobNode);
            compareWithNodeData(jobOutputNode, outputHHPath);
        } catch (KeeperException | JAXBException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void compareWithNodeData(String node, String data) {
        String configNodeData = getConfigNodeData(node);
        if (configNodeData.equals(data)) {
            throw new RuntimeException(node + " data inconsistent");
        }
    }

    public static void uploadJobEntity(String jobUUID, int jobEntityId, JobEntity jobEntity) {
        try {
            String jobEntityIdNode = getJobEntityIdNode(jobUUID, jobEntityId+"");
            createZKNode(jobEntityIdNode, jobEntity);
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    private static void createZKNode(String node, Object data) {
        CountDownLatch signal = new CountDownLatch(1);
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            manager.createPersistentNode(node, signal, data);
            signal.await();
        } catch (IOException | JAXBException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
