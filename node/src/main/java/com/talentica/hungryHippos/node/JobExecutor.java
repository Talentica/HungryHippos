/**
 * 
 */
package com.talentica.hungryHippos.node;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * NodeStarter will accept the sharded data and do various operations i.e row count per job and also
 * execution of the aggregation of the data.
 * 
 * Created by debasishc on 1/9/15.
 */
public class JobExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);

  private static NodesManager nodesManager;

  private static DataStore dataStore;

  private static String PRIFIX_NODE_NAME = "_node";

  private static String jobUUId;

  public static void main(String[] args) {
    try {
      LOGGER.info("Start Node initialize");
      validateArguments(args);
      setContext(args);
      initialize(args);
      long startTime = System.currentTimeMillis();
      listenerOnJobMatrix();
      JobRunner jobRunner = createJobRunner();
      List<JobEntity> jobEntities = getJobsFromZKNode();
      for (JobEntity jobEntity : jobEntities) {
        Object[] loggerJobArgument = new Object[] {jobEntity.getJobId()};
        LOGGER.info("Starting execution of job: {}", loggerJobArgument);
        jobRunner.run(jobEntity);
        LOGGER.info("Finished with execution of job: {}", loggerJobArgument);
      }
      jobEntities.clear();
      sendFinishJobMatrixSignal();
      long endTime = System.currentTimeMillis();
      LOGGER
          .info("It took {} seconds of time to execute all jobs.", ((endTime - startTime) / 1000));
      LOGGER.info("ALL JOBS ARE FINISHED");
    } catch (Exception exception) {
      LOGGER.error("Error occured while executing node starter program.", exception);
      try {
        sendFailureSignal(nodesManager);
      } catch (IOException | InterruptedException e) {
        LOGGER.info("Unable to create the node for signal FINISH_JOB_FAILED due to {}", e);
      }
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      throw new RuntimeException(
          "Missing zookeeper xml configuration file path arguments.");
    }
  }

  /**
   * @throws Exception
   * @throws KeeperException
   * @throws InterruptedException
   */
  private static void listenerOnJobMatrix() throws Exception, KeeperException, InterruptedException {
    ZkSignalListener.waitForSignal(JobExecutor.nodesManager,
        CommonUtil.ZKJobNodeEnum.START_JOB_MATRIX.getZKJobNode());
  }

  /**
   * @param args
   * @throws JAXBException
   * @throws FileNotFoundException
   */
  private static void initialize(String[] args) throws FileNotFoundException, JAXBException {
    jobUUId = CoordinationApplicationContext.getZkCoordinationConfigCache().getCommonConfig().getJobuuid();
    ZkSignalListener.jobuuidInBase64 = CommonUtil.getJobUUIdInBase64(jobUUId);
    nodesManager = NodesManagerContext.getNodesManagerInstance();
  }

  /**
   * @throws IOException
   * @throws InterruptedException
   */
  private static void sendFinishJobMatrixSignal() throws IOException, InterruptedException {
    String buildFinishPath =
        ZKUtils.buildNodePath(ZkSignalListener.jobuuidInBase64) + PathUtil.SEPARATOR_CHAR
            + ("_node" + NodeUtil.getNodeId()) + PathUtil.SEPARATOR_CHAR
            + CommonUtil.ZKJobNodeEnum.FINISH_JOB_MATRIX.name();
    CountDownLatch signal = new CountDownLatch(1);
    nodesManager.createPersistentNode(buildFinishPath, signal);
    signal.await();
  }

  /**
   * @param nodesManager
   * @throws IOException
   * @throws InterruptedException
   */
  private static void sendFailureSignal(NodesManager nodesManager) throws IOException,
      InterruptedException {
    String basePathPerNode =
        NodesManagerContext.getZookeeperConfiguration().getZookeeperDefaultSetting().getHostPath()
            + PathUtil.SEPARATOR_CHAR + ZkSignalListener.jobuuidInBase64 + PathUtil.SEPARATOR_CHAR
            + (PRIFIX_NODE_NAME + NodeUtil.getNodeId()) + PathUtil.SEPARATOR_CHAR
            + CommonUtil.ZKJobNodeEnum.FINISH_JOB_FAILED.getZKJobNode();
    CountDownLatch signal = new CountDownLatch(1);
    nodesManager.createPersistentNode(basePathPerNode, signal);
    signal.await();
    ZkSignalListener.createErrorEncounterSignal(nodesManager);
  }

  /**
   * Create the job runner.
   * 
   * @return JobRunner
   * @throws IOException
   * @throws JAXBException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ClassNotFoundException
   */
  private static JobRunner createJobRunner() throws IOException, ClassNotFoundException,
      KeeperException, InterruptedException, JAXBException {
    FieldTypeArrayDataDescription dataDescription =
        CoordinationApplicationContext.getConfiguredDataDescription();
    dataDescription.setKeyOrder(CoordinationApplicationContext.getShardingDimensions());
    dataStore =
        new FileDataStore(NodeUtil.getKeyToValueToBucketMap().size(), dataDescription, true);
    return new JobRunner(dataDescription, dataStore);
  }

  /**
   * Get the list of jobs from ZK Node.
   * 
   * @return List<Job>
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private static List<JobEntity> getJobsFromZKNode() throws IOException, ClassNotFoundException,
      InterruptedException, KeeperException {

    String buildPath =
        ZKUtils.buildNodePath(CommonUtil.getJobUUIdInBase64(jobUUId)) + PathUtil.SEPARATOR_CHAR
            + ("_node" + NodeUtil.getNodeId()) + PathUtil.SEPARATOR_CHAR
            + CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name();
    Set<LeafBean> leafs = ZKUtils.searchLeafNode(buildPath, null, null);
    LOGGER.info("Leafs size found {}", leafs.size());
    List<JobEntity> jobEntities = new ArrayList<JobEntity>();
    for (LeafBean leaf : leafs) {
      LOGGER.info("Leaf path {} and name {}", leaf.getPath(), leaf.getName());
      String buildLeafPath = ZKUtils.getNodePath(leaf.getPath(), leaf.getName());
      LOGGER.info("Build path {}", buildLeafPath);
      LeafBean leafObject = ZKUtils.getNodeValue(buildPath, buildLeafPath, leaf.getName(), null);
      JobEntity jobEntity = (JobEntity) leafObject.getValue();
      if (jobEntity == null)
        continue;
      jobEntities.add(jobEntity);
    }
    LOGGER.info("TOTAL JOBS FOUND {}", jobEntities.size());
    return jobEntities;
  }

  private static void setContext(String[] args) {
    NodesManagerContext.setZookeeperXmlPath(args[0]);
  }

}
