package com.talentica.hungryHippos.job.main;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.JobConfigPublisher;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.job.util.JobIDGenerator;
import com.talentica.hungryHippos.job.util.JobJarPublisher;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;

/**
 * This method is for client to instantiate Jobs Created by rajkishoreh on 2/8/16.
 */
public class JobOrchestrator {

  private static final Logger logger = LoggerFactory.getLogger(JobOrchestrator.class);
  private static HungryHippoCurator curator;

  /**
   * This the entry point of the class
   *
   * @param args
   * @throws IOException
   * @throws InterruptedException
   * @throws JAXBException
   */
  public static void main(String[] args) {
    logger.info("Started JobOrchestrator");
    long startTime = System.currentTimeMillis();
    int jobStatus = -1;
    validateArguments(args);
    String clientConfigPath = args[0];
    String localJarPath = args[1];
    String jobMatrixClass = args[2];
    String inputHHPath = args[3];
    String outputHHPath = args[4];
    String outputHHPathNode = null;

    try {

      ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, ClientConfig.class);
      int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
      String connectString = clientConfig.getCoordinationServers().getServers();
      String userName = clientConfig.getOutput().getNodeSshUsername();
      curator = HungryHippoCurator.getInstance(connectString, sessionTimeOut);

      FileSystemUtils.validatePath(inputHHPath, true);
      FileSystemUtils.validatePath(outputHHPath, true);
      validateOutputHHPath(outputHHPath);
      HungryHipposFileSystem hhfs = HungryHipposFileSystem.getInstance();
      hhfs.validateFileDataReady(inputHHPath);
      logger.info("Data synchronized time in ms {}",(System.currentTimeMillis() - startTime));
      startTime = System.currentTimeMillis();
      String fsRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
          .getZookeeperDefaultConfig().getFilesystemPath();
      boolean isSharded = curator.checkExists(fsRootNode + inputHHPath
          + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.SHARDED);
      if (!isSharded) {
        throw new RuntimeException("Not sharded file. Can not run the jobs");
      }

      outputHHPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
          .getZookeeperDefaultConfig().getFilesystemPath() + outputHHPath;
      curator.createPersistentNode(outputHHPathNode, FileSystemConstants.IS_A_FILE);
      String jobUUID = JobIDGenerator.generateJobID();
      logger.info("Publishing Jar for Job {}", jobUUID);
      boolean isJarPublished = JobJarPublisher.publishJar(jobUUID, localJarPath, userName);
      logger.info("Published Jar for Job {} : {}", jobUUID, isJarPublished);
      boolean isConfigPublished = false;
      if (isJarPublished) {
        logger.info("Publishing Configurations for Job {}", jobUUID);
        isConfigPublished =
            JobConfigPublisher.publish(jobUUID, jobMatrixClass, inputHHPath, outputHHPath);
        logger.info("Published Configurations for Job {} : {}", jobUUID, isConfigPublished);
      }
      if (isConfigPublished) {
        jobStatus = runJobManager(clientConfigPath, localJarPath, jobMatrixClass, jobUUID);
      }
      if (jobStatus != 0) {
        logger.error("Job for {} Failed", jobUUID);
        throw new RuntimeException("Job Failed");
      } else {
        String dataReadyNode = outputHHPathNode + "/" + FileSystemConstants.DATA_READY;
        curator.createPersistentNode(dataReadyNode);
        logger.info("Job {} Completed Successfully", jobUUID);
        long endTime = System.currentTimeMillis();
        logger.info("It took {} seconds of time to for running all jobs (excluding data sync) ",
            ((endTime - startTime) / 1000));
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

  }

  /**
   * Spawns a new process for running the JobManagerStarter
   *
   * @param clientConfigPath
   * @param localJarPath
   * @param jobMatrixClass
   * @param jobUUID
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public static int runJobManager(String clientConfigPath, String localJarPath,
      String jobMatrixClass, String jobUUID) throws IOException, InterruptedException {
    ProcessBuilder jobManagerProcessBuilder = new ProcessBuilder("java",
        JobManagerStarter.class.getName(), clientConfigPath, localJarPath, jobMatrixClass, jobUUID);
    String logDir = FileUtils.getUserDirectoryPath() + File.separator + "temp" + File.separator
        + "hungryhippos" + File.separator + "logs" + File.separator + jobUUID + File.separator;
    File errLogFile = FileSystemUtils
        .createNewFile(logDir + JobManagerStarter.class.getName().toLowerCase() + ".err");
    jobManagerProcessBuilder.redirectError(errLogFile);
    logger.info("stderr Log file : " + errLogFile.getAbsolutePath());
    File outLogFile = FileSystemUtils
        .createNewFile(logDir + JobManagerStarter.class.getName().toLowerCase() + ".out");
    jobManagerProcessBuilder.redirectOutput(outLogFile);
    logger.info("stdout Log file : " + outLogFile.getAbsolutePath());
    Process jobManagerProcess = jobManagerProcessBuilder.start();
    int jobStatus = jobManagerProcess.waitFor();
    return jobStatus;
  }

  /**
   * Validates the arguements
   *
   * @param args
   */
  private static void validateArguments(String[] args) {
    if (args.length < 5) {
      System.out.println(
          "Missing {zookeeper xml configuration} or {local JarPath} or {JobMatrix Class name} "
              + "or {Job input path} or {Job output path} arguments.");
      System.exit(1);
    }
  }

  private static void validateOutputHHPath(String outputHHPath) throws HungryHippoException {
    if ("".equals(outputHHPath)) {
      throw new RuntimeException("Empty output path");
    }
    String outputHHPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + outputHHPath;
    String dataReadyNode = outputHHPathNode + "/" + FileSystemConstants.DATA_READY;
    boolean nodeExists = curator.checkExists(dataReadyNode);
    if (nodeExists) {
      throw new RuntimeException(outputHHPath + " already exists");
    }
  }
}
