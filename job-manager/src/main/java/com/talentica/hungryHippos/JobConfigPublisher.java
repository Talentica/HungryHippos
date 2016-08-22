package com.talentica.hungryHippos;

import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.getConfigNodeData;
import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.getJobClassNode;
import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.getJobEntityIdNode;
import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.getJobInputNode;
import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.getJobNode;
import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.getJobOutputNode;

import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.JobEntity;

/**
 * This class is for Client to publish Job Configurations Created by rajkishoreh on 2/8/16.
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
  public static boolean publish(String jobUUID, String jobMatrixClass, String inputHHPath,
      String outputHHPath) {
    try {
      String jobNode = getJobNode(jobUUID);
      ZkUtils.createZKNode(jobNode, "");
      String jobClassNode = getJobClassNode(jobNode);
      ZkUtils.createZKNode(jobClassNode, jobMatrixClass);
      String jobInputNode = getJobInputNode(jobNode);
      ZkUtils.createZKNode(jobInputNode, inputHHPath);
      String jobOutputNode = getJobOutputNode(jobNode);
      ZkUtils.createZKNode(jobOutputNode, outputHHPath);
      validateConfigNodes(jobUUID, jobMatrixClass, inputHHPath, outputHHPath);
      return true;
    } catch (Exception e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  /**
   * Validates configuration data
   * 
   * @param jobUUID
   * @param jobMatrixClass
   * @param inputHHPath
   * @param outputHHPath
   */
  private static void validateConfigNodes(String jobUUID, String jobMatrixClass, String inputHHPath,
      String outputHHPath) {
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
    } catch (JAXBException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Compares with Node data
   * 
   * @param node
   * @param data
   */
  private static void compareWithNodeData(String node, String data) {
    String configNodeData = getConfigNodeData(node);
    if (!configNodeData.equals(data)) {
      throw new RuntimeException(node + " data inconsistent");
    }
  }

  /**
   * Uploads List of JobEntities
   * 
   * @param jobUUID
   * @param jobEntities
   */
  public static void uploadJobEntities(String jobUUID, List<JobEntity> jobEntities) {
    for (JobEntity jobEntity : jobEntities) {
      uploadJobEntity(jobUUID, jobEntity);
    }
  }

  /**
   * Uploads JobEntity Object
   * 
   * @param jobUUID
   * @param jobEntityId
   * @param jobEntity
   */
  public static void uploadJobEntity(String jobUUID, JobEntity jobEntity) {
    try {
      String jobNode = getJobNode(jobUUID);
      String jobEntityIdNode = getJobEntityIdNode(jobNode, jobEntity.getJobId() + "");
      ZkUtils.createZKNode(jobEntityIdNode, null);
    } catch (Exception e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

}
