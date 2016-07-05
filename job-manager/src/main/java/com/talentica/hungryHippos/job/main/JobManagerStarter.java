package com.talentica.hungryHippos.job.main;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.master.job.JobManager;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.client.CoordinationServers;

/**
 * @author PooshanS
 *
 */
public class JobManagerStarter {

  /**
   * @param args
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(JobManagerStarter.class);
  private static NodesManager nodesManager;
  private static String jobUUId;

  public static void main(String[] args) {
    try {
      validateProgramArguments(args);
      ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(args[2], ClientConfig.class);
      CoordinationServers coordinationServers = clientConfig.getCoordinationServers();
      initialize(args, coordinationServers);
      long startTime = System.currentTimeMillis();
      JobManager jobManager = new JobManager();
      JobManager.nodesManager = NodesManagerContext.getNodesManagerInstance();
      jobManager.addJobList(((JobMatrix) getJobMatrix(args)).getListOfJobsToExecute());
      jobManager.start(jobUUId);
      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to for running all jobs.",
          ((endTime - startTime) / 1000));
    } catch (Exception exception) {
      errorHandler(exception);
    }
  }

  /**
   * @param exception
   */
  private static void errorHandler(Exception exception) {
    LOGGER.error("Error occured while executing master starter program.", exception);
    try {
      ZkSignalListener.createErrorEncounterSignal(nodesManager);
    } catch (IOException | InterruptedException e) {
      LOGGER.info("Unable to create the node on zk due to {}", e.getMessage());
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  private static void initialize(String[] args, CoordinationServers coordinationServers)
      throws Exception {
    jobUUId = args[1];
    LOGGER.info("Job UUID is {}", jobUUId);
    CommonUtil.loadDefaultPath(jobUUId);
    ZkSignalListener.jobuuidInBase64 = CommonUtil.getJobUUIdInBase64(jobUUId);
    JobManagerStarter.nodesManager = NodesManagerContext.getNodesManagerInstance();
  }

  private static void validateProgramArguments(String[] args) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    if (args.length < 3) {
      System.out
          .println("Please provide the required jobn matrix class name as 1st argument to be able to run the program, 2nd argument as jobuuid and 3rd argument of client configuration file path.");
      System.exit(1);
    }
    Object jobMatrix = getJobMatrix(args);
    if (!(jobMatrix instanceof JobMatrix)) {
      System.out
          .println("First argument should be a valid job matrix class name in the class-path.");
      System.exit(1);
    }
  }

  private static Object getJobMatrix(String[] args) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    Object jobMatrix = Class.forName(args[0]).newInstance();
    return jobMatrix;
  }

}
