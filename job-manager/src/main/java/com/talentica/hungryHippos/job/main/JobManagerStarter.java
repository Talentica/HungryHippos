package com.talentica.hungryHippos.job.main;

import java.io.FileNotFoundException;
import java.net.URLClassLoader;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.master.job.JobManager;
import com.talentica.hungryHippos.utility.ClassLoaderUtil;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;

/**
 * @author PooshanS
 *
 */
public class JobManagerStarter {

  /**
   * @param args
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(JobManagerStarter.class);
  private static HungryHippoCurator curator;

  public static void main(String[] args) {
    System.out.println("In Job Manager Main.");
    try {
      validateArguments(args);
      String clientConfigPath = args[0];
      String localJarPath = args[1];
      String jobMatrixClass = args[2];
      String jobUUId = args[3];

      System.out.println("Job Manager Started.");
      ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, ClientConfig.class);
      String connectString = clientConfig.getCoordinationServers().getServers();
      int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
      curator = HungryHippoCurator.getInstance(connectString, sessionTimeOut);
      Object jobMatrix = getJobMatrix(localJarPath, jobMatrixClass);
      validateJobMatrixClass(jobMatrix);
      initialize(jobUUId);
      long startTime = System.currentTimeMillis();
      JobManager jobManager = new JobManager();
      jobManager.addJobList(((JobMatrix) jobMatrix).getListOfJobsToExecute());
      jobManager.start(jobUUId);
      long endTime = System.currentTimeMillis();
      System.out.println("Job Manager Completed.");
      LOGGER.info("It took {} seconds of time to for running all jobs.",
          ((endTime - startTime) / 1000));

    } catch (Exception exception) {
      System.exit(1);
    }
  }



  /**
   *
   * @param jobUUId
   * @throws Exception
   */
  private static void initialize(String jobUUId) throws Exception {
    LOGGER.info("Job UUID is {}", jobUUId);
    ZkSignalListener.jobuuidInBase64 = jobUUId;
  }

  private static void validateJobMatrixClass(Object jobMatrix) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, FileNotFoundException, JAXBException {

    if (!(jobMatrix instanceof JobMatrix)) {
      System.out.println("Please provide the job matrix class name in job configuration xml file.");
      System.exit(1);
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 4) {
      System.out.println(
          "Missing {zookeeper xml configuration} or {local JarPath} or {JobMatrix Class name} or {Job UUID} arguments.");
      System.exit(1);
    }
  }

  private static Object getJobMatrix(String localJarPath, String jobMatrixClass)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException,
      FileNotFoundException, JAXBException {
    URLClassLoader classLoader = ClassLoaderUtil.getURLClassLoader(localJarPath);
    Object jobMatrix = Class.forName(jobMatrixClass, true, classLoader).newInstance();
    return jobMatrix;
  }

}
