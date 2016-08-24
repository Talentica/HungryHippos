package com.talentica.hungryHippos.job.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.JobConfigPublisher;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.job.util.JobIDGenerator;
import com.talentica.hungryHippos.job.util.JobJarPublisher;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;

/**
 * This method is for client to instantiate Jobs Created by rajkishoreh on 2/8/16.
 */
public class JobOrchestrator {

  private static final Logger logger = LoggerFactory.getLogger(JobOrchestrator.class);

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
    int jobStatus = -1;
    validateArguments(args);
    String clientConfigPath = args[0];
    String localJarPath = args[1];
    String jobMatrixClass = args[2];
    String inputHHPath = args[3];
    String outputHHPath = args[4];
    String outputHHPathNode = null;
    try {
      NodesManagerContext.getNodesManagerInstance(clientConfigPath);
      FileSystemUtils.validatePath(inputHHPath, true);
      FileSystemUtils.validatePath(outputHHPath, true);
      validateOutputHHPath(outputHHPath);
      HungryHipposFileSystem.validateFileDataReady(inputHHPath);
      String fsRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
          .getZookeeperDefaultConfig().getFilesystemPath();
      boolean isSharded = ZkUtils.checkIfNodeExists(fsRootNode + inputHHPath
          + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.SHARDED);
      if (!isSharded) {
        throw new RuntimeException("Not sharded file. Can not run the jobs");
      }
      outputHHPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
          .getZookeeperDefaultConfig().getFilesystemPath() + outputHHPath;
      ZkUtils.createFileNode(outputHHPathNode);
      String jobUUID = JobIDGenerator.generateJobID();
      logger.info("Publishing Jar for Job {}", jobUUID);
      boolean isJarPublished = JobJarPublisher.publishJar(jobUUID, localJarPath);
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
        ZkUtils.createZKNode(dataReadyNode, "");
        logger.info("Job {} Completed Successfully", jobUUID);
      }
    } catch (Exception e) {
      logger.error(e.toString());
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
    ProcessBuilder jobManagerProcessBuilder = new ProcessBuilder("java",JobManagerStarter.class.getName(), clientConfigPath, localJarPath, jobMatrixClass, jobUUID);
    Process jobManagerProcess = jobManagerProcessBuilder.start();
   int jobStatus = jobManagerProcess.waitFor();
  BufferedReader br = new BufferedReader(new InputStreamReader(jobManagerProcess.getErrorStream()));
  String line = null;
  StringBuilder sb = new StringBuilder();
  while ((line = br.readLine()) != null){
      sb.append(line).append("\n");
  }
  System.out.println("Console OUTPUT : \n"+sb.toString());
    logger.info("JobManager started for Job " + jobUUID);
   
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

  private static void validateOutputHHPath(String outputHHPath) {
    if ("".equals(outputHHPath)) {
      throw new RuntimeException("Empty output path");
    }
    String outputHHPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + outputHHPath;
    String dataReadyNode = outputHHPathNode + "/" + FileSystemConstants.DATA_READY;
    boolean nodeExists = ZkUtils.checkIfNodeExists(dataReadyNode);
    if (nodeExists) {
      throw new RuntimeException(outputHHPath + " already exists");
    }
  }
}
