package com.talentica.hungryHippos.job.main;

import com.talentica.hungryHippos.JobConfigPublisher;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.job.util.JobIDGenerator;
import com.talentica.hungryHippos.job.util.JobJarPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;

/**
 * This method is for client to instantiate Jobs
 * Created by rajkishoreh on 2/8/16.
 */
public class JobOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(JobOrchestrator.class);

    /**
     * This the entry point of the class
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws JAXBException
     */
    public static void main(String[] args) throws IOException, InterruptedException, JAXBException {

        int jobStatus = -1;
        validateArguments(args);
        String clientConfigPath = args[0];
        String localJarPath = args[1];
        String jobMatrixClass = args[2];
        String inputHHPath = args[3];
        String outputHHPath = args[4];
        NodesManager manager = NodesManagerContext.getNodesManagerInstance(clientConfigPath);
        String jobUUID = JobIDGenerator.generateJobID();
        logger.info("Publishing Jar for Job {}", jobUUID);
        boolean isJarPublished = JobJarPublisher.publishJar(jobUUID, localJarPath);
        logger.info("Published Jar for Job {} : {}", jobUUID,isJarPublished);
        boolean isConfigPublished = false;
        if (isJarPublished) {
            logger.info("Publishing Configurations for Job {}", jobUUID);
            isConfigPublished = JobConfigPublisher.publish(jobUUID,jobMatrixClass, inputHHPath, outputHHPath);
            logger.info("Published Configurations for Job {} : {}",jobUUID, isConfigPublished);
        }
        if (isConfigPublished) {
            jobStatus = runJobManager(clientConfigPath,jobUUID, jobMatrixClass, localJarPath);
        }
        if (jobStatus != 0) {
            logger.error("Job for {} Failed",jobUUID);
            System.exit(1);
        } else {
            logger.info("Job {} Completed Successfully",jobUUID);
        }

    }

    /**
     * Spawns a new process for running the JobManagerStarter
     * @param clientConfigPath
     * @param localJarPath
     * @param jobMatrixClass
     * @param jobUUID
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static int runJobManager(String clientConfigPath, String localJarPath, String jobMatrixClass, String jobUUID) throws IOException, InterruptedException {
        ProcessBuilder jobManagerProcessBuilder = new ProcessBuilder("java", JobManagerStarter.class.getName(),
                clientConfigPath,localJarPath, jobMatrixClass, jobUUID);
        Process jobManagerProcess = jobManagerProcessBuilder.start();
        logger.info("JobManager started for Job " + jobUUID);
        int jobStatus = jobManagerProcess.waitFor();
        return jobStatus;
    }

    /**
     * Validates the arguements
     * @param args
     */
    private static void validateArguments(String[] args) {
        if (args.length < 5) {
            System.out.println("Missing {zookeeper xml configuration} or {local JarPath} or {JobMatrix Class name} " +
                            "or {Job input path} or {Job output path} arguments.");
            System.exit(1);
        }
    }
}
