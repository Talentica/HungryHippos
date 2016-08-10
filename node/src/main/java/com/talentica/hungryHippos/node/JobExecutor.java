/**
 * 
 */
package com.talentica.hungryHippos.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.common.context.JobRunnerApplicationContext;
import com.talentica.hungryHippos.common.util.ClassLoaderUtil;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.node.job.JobConfigReader;
import com.talentica.hungryHippos.node.job.JobStatusNodeCoordinator;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.JobEntity;

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

  private static String inputHHPath;

  private static String outputHHPath;

  private static String PRIFIX_NODE_NAME = "_node";

  private static String jobUUId;
  
  private static ShardingApplicationContext context;

  public static void main(String[] args) {
    try {
      LOGGER.info("Start Node initialize");
      validateArguments(args);
      initialize(args);
      loadClasses();
      context = new ShardingApplicationContext(inputHHPath);
      long startTime = System.currentTimeMillis();
      JobRunner jobRunner = createJobRunner();
      int nodeId =  NodeInfo.INSTANCE.getIdentifier();
      JobStatusNodeCoordinator.updateInProgressJob(jobUUId,nodeId);
      List<JobEntity> jobEntities = getJobsFromZKNode();
      for (JobEntity jobEntity : jobEntities) {
        int jobEntityId = jobEntity.getJobId();
        JobStatusNodeCoordinator.updateStartedJobEntity(jobUUId,jobEntityId,nodeId);
        Object[] loggerJobArgument = new Object[] {jobEntityId};
        LOGGER.info("Starting execution of job: {}", loggerJobArgument);
        jobRunner.run(jobEntity);
        LOGGER.info("Finished with execution of job: {}", loggerJobArgument);
        JobStatusNodeCoordinator.updateCompletedJobEntity(jobUUId,jobEntityId,nodeId);
      }
      jobEntities.clear();
      JobStatusNodeCoordinator.updateNodeJobCompleted(jobUUId,nodeId);
      long endTime = System.currentTimeMillis();
      LOGGER
          .info("It took {} seconds of time to execute all jobs.", ((endTime - startTime) / 1000));
      LOGGER.info("ALL JOBS ARE FINISHED");
    } catch (Exception exception) {
      LOGGER.error("Error occured while executing node starter program.", exception);
      System.exit(1);
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 2) {
      throw new RuntimeException(
              "Either missing 1st argument {zookeeper configuration} or 2nd argument {coordination configuration}");
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
  private static void initialize(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException {
    String clientConfigPath = args[0];
    jobUUId = args[1];
    nodesManager = NodesManagerContext.getNodesManagerInstance(clientConfigPath);
    ZkSignalListener.jobuuidInBase64 = jobUUId;
    inputHHPath = JobConfigReader.readInputPath(jobUUId);
    outputHHPath = JobConfigReader.readOutputPath(jobUUId);

  }

  /**
   * Loads the required classes
   * @throws ClassNotFoundException
     */
  private static void loadClasses() throws ClassNotFoundException {
    String jarDirectory = JobRunnerApplicationContext.getZkJobRunnerConfig().getJarRootDirectory();
    String jobJarPath = jarDirectory+ File.pathSeparator+jobUUId;
    URLClassLoader urlClassLoader = ClassLoaderUtil.getURLClassLoader(jobJarPath);
    String className = JobConfigReader.readClassName(jobUUId);
    Class classToLoad = Class.forName(className,true,urlClassLoader);
    LOGGER.info("Loaded {} class successfully",classToLoad.getName());
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
        context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    NodeUtil nodeUtil = new NodeUtil(inputHHPath);
    dataStore = new FileDataStore(nodeUtil.getKeyToValueToBucketMap().size(), dataDescription,
        inputHHPath, NodeInfo.INSTANCE.getId(), true,context);
    return new JobRunner(dataDescription, dataStore, NodeInfo.INSTANCE.getId(), outputHHPath);
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
    List<JobEntity> jobEntities = JobConfigReader.getJobEntityList(jobUUId);
    LOGGER.info("TOTAL JOBS FOUND {}", jobEntities.size());
    return jobEntities;
  }
  
  public static ShardingApplicationContext getShardingApplicationContext(){
    return context;
  }


}
