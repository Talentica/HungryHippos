/**
 * 
 */
package com.talentica.hungryHippos.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.common.context.JobRunnerApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.node.job.JobConfigReader;
import com.talentica.hungryHippos.node.job.JobStatusNodeCoordinator;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.ClassLoaderUtil;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * NodeStarter will accept the sharded data and do various operations i.e row count per job and also
 * execution of the aggregation of the data.
 * 
 * Created by debasishc on 1/9/15.
 */
public class JobExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);

  private static DataStore dataStore;

  private static String inputHHPath;

  private static String outputHHPath;

  private static String jobUUId;

  private static ShardingApplicationContext context;

  public static void main(String[] args) {
    try {
      LOGGER.info("Start Node initialize");
      validateArguments(args);
      initialize(args);
      String dataAbsolutePath = FileSystemContext.getRootDirectory() + inputHHPath;
      String shardingTableFolderPath = dataAbsolutePath+ File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
      context = new ShardingApplicationContext(shardingTableFolderPath);
      long startTime = System.currentTimeMillis();
      JobRunner jobRunner = createJobRunner();
      int nodeId = NodeInfo.INSTANCE.getIdentifier();
      JobStatusNodeCoordinator.updateInProgressJob(jobUUId, nodeId);
      String jarRootDirectory = JobRunnerApplicationContext.getZkJobRunnerConfig().getJarRootDirectory();
      String jobJarPath = jarRootDirectory + File.separatorChar + jobUUId;
      String className = JobConfigReader.readClassName(jobUUId); 
      List<JobEntity> jobEntities = getJobEntities(jobJarPath,className);
      for (JobEntity jobEntity : jobEntities) {
        int jobEntityId = jobEntity.getJobId();
        JobStatusNodeCoordinator.updateStartedJobEntity(jobUUId, jobEntityId, nodeId);
        Object[] loggerJobArgument = new Object[] {jobEntityId};
        LOGGER.info("Starting execution of job: {}", loggerJobArgument);
        jobRunner.run(jobEntity);
        LOGGER.info("Finished with execution of job: {}", loggerJobArgument);
        JobStatusNodeCoordinator.updateCompletedJobEntity(jobUUId, jobEntityId, nodeId);
      }
      jobEntities.clear();
      JobStatusNodeCoordinator.updateNodeJobCompleted(jobUUId, nodeId);
      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to execute all jobs.",
          ((endTime - startTime) / 1000));
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
   * @param args
   * @throws JAXBException
   * @throws FileNotFoundException
   */
  private static void initialize(String[] args)
      throws FileNotFoundException, JAXBException, ClassNotFoundException {
    String clientConfigPath = args[0];
    jobUUId = args[1];
    NodesManagerContext.getNodesManagerInstance(clientConfigPath);
    ZkSignalListener.jobuuidInBase64 = jobUUId;
    inputHHPath = JobConfigReader.readInputPath(jobUUId);
    outputHHPath = JobConfigReader.readOutputPath(jobUUId);

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
    FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    NodeUtil nodeUtil = new NodeUtil(inputHHPath);
    dataStore = new FileDataStore(nodeUtil.getKeyToValueToBucketMap().size(), dataDescription,
        inputHHPath, NodeInfo.INSTANCE.getId(), true, context);
    return new JobRunner(dataDescription, dataStore, NodeInfo.INSTANCE.getId(), outputHHPath);
  }

  public static ShardingApplicationContext getShardingApplicationContext() {
    return context;
  }

  private static List<JobEntity> getJobEntities(String localJarPath, String jobMatrixClass)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException,
      FileNotFoundException, JAXBException {
    List<JobEntity> jobEntities = new ArrayList<>();
    URLClassLoader classLoader = ClassLoaderUtil.getURLClassLoader(localJarPath);
    JobMatrix jobMatrix =
        (JobMatrix) Class.forName(jobMatrixClass, true, classLoader).newInstance();

    JobEntity jobEntity;
    for (Job job : jobMatrix.getListOfJobsToExecute()) {
      jobEntity = new JobEntity();
      jobEntity.setJob(job);
      jobEntities.add(jobEntity);
    }

    return jobEntities;
  }


}
