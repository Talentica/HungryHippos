package com.talentica.hungryHippos.job.main;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.job.context.JobManagerApplicationContext;
import com.talentica.hungryHippos.master.job.JobManager;

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
      validateArguments(args);
      setContext(args);
      validateJobMatrixClass();
      initialize();
      long startTime = System.currentTimeMillis();
      JobManager jobManager = new JobManager();
      JobManager.nodesManager = NodesManagerContext.getNodesManagerInstance();
      jobManager.addJobList(((JobMatrix) getJobMatrix()).getListOfJobsToExecute());
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
  private static void initialize() throws Exception {
    jobUUId = CoordinationApplicationContext.getZkCoordinationConfigCache().getCommonConfig().getJobuuid();
    LOGGER.info("Job UUID is {}", jobUUId);
    ZkSignalListener.jobuuidInBase64 = CommonUtil.getJobUUIdInBase64(jobUUId);
    JobManagerStarter.nodesManager = NodesManagerContext.getNodesManagerInstance();
  }

  private static void validateJobMatrixClass() throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, FileNotFoundException, JAXBException {
    Object jobMatrix = getJobMatrix();
    if (!(jobMatrix instanceof JobMatrix)) {
      System.out.println("Please provide the job matrix class name in job configuration xml file.");
      System.exit(1);
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      System.out
          .println("Missing zookeeper xml configuration file path arguments.");
      System.exit(1);
    }
  }

  private static Object getJobMatrix() throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, FileNotFoundException, JAXBException {
    Object jobMatrix =
        Class.forName(JobManagerApplicationContext.getZkJobConfigCache().getClassName()).newInstance();
    return jobMatrix;
  }

  private static void setContext(String[] args) {
    NodesManagerContext.setZookeeperXmlPath(args[0]);
  }

}
