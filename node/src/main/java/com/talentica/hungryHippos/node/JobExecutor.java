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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.common.context.JobRunnerApplicationContext;

import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;

import com.talentica.hungryHippos.node.job.JobConfigReader;
import com.talentica.hungryHippos.node.job.JobRunner;
import com.talentica.hungryHippos.node.job.JobStatusNodeCoordinator;
import com.talentica.hungryHippos.node.job.SortedDataJobRunner;
import com.talentica.hungryHippos.node.job.UnsortedDataJobRunner;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.ClassLoaderUtil;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * {@code JobExecutor} will accept the sharded data and do various operations i.e row count per job
 * and also execution of the aggregation of the data.
 * 
 * @author debasishc
 * @since 1/9/15.
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
      String dataFolderPath = FileSystemContext.getRootDirectory() + inputHHPath;
      String shardingTableFolderPath =
          dataFolderPath + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
      context = new ShardingApplicationContext(shardingTableFolderPath);
      long startTime = System.currentTimeMillis();
      JobRunner jobRunner = createJobRunner(dataFolderPath);
      int nodeId = NodeInfo.INSTANCE.getIdentifier();
      String jobRootDirectory =
          JobRunnerApplicationContext.getZkJobRunnerConfig().getJobsRootDirectory();
      String jobJarPath = jobRootDirectory + File.separatorChar + jobUUId + File.separator + "lib";
      String className = JobConfigReader.readClassName(jobUUId);
      List<JobEntity> jobEntities = getJobEntities(jobJarPath, className);
      List<PrimaryDimensionwiseJobsCollection> jobsCollectionList =
          PrimaryDimensionwiseJobsCollection.from(jobEntities, context);
      jobRunner.run(jobUUId, jobsCollectionList);
      JobStatusNodeCoordinator.updateNodeJobCompleted(jobUUId, nodeId);
      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to execute all jobs.",
          ((endTime - startTime) / 1000));
      LOGGER.info("ALL JOBS ARE FINISHED");
    } catch (Exception exception) {
      JobStatusNodeCoordinator.updateNodeJobFailed(jobUUId, NodeInfo.INSTANCE.getIdentifier());
      LOGGER.error("Error occured while executing node starter program.", exception);
      System.exit(1);
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 2) {
      throw new RuntimeException(
          "Either missing 1st argument {path to client configuration xml} or 2nd argument {job uuid}");
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
    ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, ClientConfig.class);
    int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
    String connectString = clientConfig.getCoordinationServers().getServers();
    HungryHippoCurator.getInstance(connectString, sessionTimeOut);

    inputHHPath = JobConfigReader.readInputPath(jobUUId);
    outputHHPath = JobConfigReader.readOutputPath(jobUUId);

  }

  private static JobRunner createJobRunner(String inputFilePath) throws IOException {
    FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    NodeUtil nodeUtil = new NodeUtil(inputHHPath);
    dataStore = new FileDataStore(nodeUtil.getKeyToValueToBucketMap().size(), dataDescription,
        inputHHPath, NodeInfo.INSTANCE.getId(), true, context);
    boolean isSorting = context.getShardingClientConfig().isDataFileSorting();
    if (isSorting) {
      return new SortedDataJobRunner(dataDescription, dataStore, NodeInfo.INSTANCE.getId(),
          inputFilePath, outputHHPath, context);
    } else {
      return new UnsortedDataJobRunner(dataDescription, dataStore, NodeInfo.INSTANCE.getId(),
          outputHHPath);
    }
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
