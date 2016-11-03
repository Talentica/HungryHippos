package com.talentica.hungryHippos.common.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;

/**
 * This class is for having common methods related to Job Configurations
 * @author rajkishoreh 
 * @since 3/8/16.
 */
public class JobConfigCommonOperations {

  public static final Logger LOGGER = LoggerFactory.getLogger(JobConfigCommonOperations.class);

  private static final String INPUT_HH_PATH = "INPUT_HH_PATH";
  private static final String OUTPUT_HH_PATH = "OUTPUT_HH_PATH";
  private static final String CLASS_NAME = "CLASS_NAME";
  private static final String JOB_ENITY_LIST = "JOB_ENITY_LIST";
  private static final String ZK_PATH = "/";

  public static String getJobClassNode(String jobNode) {
    return jobNode + ZK_PATH + CLASS_NAME;
  }

  public static String getJobInputNode(String jobNode) {
    return jobNode + ZK_PATH + INPUT_HH_PATH;
  }

  public static String getJobOutputNode(String jobNode) {
    return jobNode + ZK_PATH + OUTPUT_HH_PATH;
  }

  public static String getJobEntityListNode(String jobNode) {
    return jobNode + ZK_PATH + JOB_ENITY_LIST;
  }

  /**
   * Returns jobEntity Node
   * 
   * @param jobNode
   * @param jobEntityId
   * @return
   */
  public static String getJobEntityIdNode(String jobNode, String jobEntityId) {
    String jobEntityListNode = getJobEntityListNode(jobNode);
    return jobEntityListNode + ZK_PATH + jobEntityId;
  }

  /**
   * Returns Job Node
   * 
   * @param jobUUID
   * @return
   */
  public static String getJobNode(String jobUUID) {
    String jobConfigsRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getJobConfigPath();
    return jobConfigsRootNode + ZK_PATH + jobUUID;
  }

  /**
   * Returns String data from config node
   * 
   * @param node
   * @return
   */
  public static String getConfigNodeData(String node) {
    String configValue = "";
    try {
      HungryHippoCurator curator = HungryHippoCurator.getInstance();
      configValue = (String) curator.getZnodeData(node);
    } catch (HungryHippoException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
    return configValue;
  }



}
