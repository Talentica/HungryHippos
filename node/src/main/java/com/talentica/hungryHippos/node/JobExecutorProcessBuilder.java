package com.talentica.hungryHippos.node;

import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getPendingHHNode;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.common.context.JobRunnerApplicationContext;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.node.job.JobStatusNodeCoordinator;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;

/**
 * This class is for Node to start instantiate JobExecutor processes
 * Created by rajkishoreh on 1/8/16.
 */
public class JobExecutorProcessBuilder {
    public static final Logger logger = LoggerFactory.getLogger(JobExecutorProcessBuilder.class);
    /**
     * This method is the entry point of the class
     * @param args
     * @throws InterruptedException
     * @throws IOException
     * @throws JAXBException
     */
    public static void main(String[] args) throws InterruptedException, IOException, JAXBException {
        logger.info("Started JobExecutorProcessBuilder");
        validateArguments(args);
        String clientConfigPath = args[0];
    NodesManagerContext.getNodesManagerInstance(clientConfigPath);
        File jobsRootDirectory = new File(JobRunnerApplicationContext.getZkJobRunnerConfig().getJobsRootDirectory());
        jobsRootDirectory.mkdirs();
        int nodeId = NodeInfo.INSTANCE.getIdentifier();
        String pendingHHNode = getPendingHHNode(nodeId);
        ZkUtils.createZKNodeIfNotPresent(pendingHHNode, ""); 
        while(true){
            List<String> jobUUIDs = JobStatusNodeCoordinator.checkNodeJobUUIDs(nodeId);
            if(jobUUIDs==null||jobUUIDs.size()==0){
                continue;
            }
            for(String jobUUID:jobUUIDs){
                logger.info("Processing "+jobUUID);
        JobStatusNodeCoordinator.updateInProgressJob(jobUUID, nodeId);
                ProcessBuilder processBuilder = new ProcessBuilder("java",JobExecutor.class.getName(),clientConfigPath,jobUUID);
                String logDir = JobRunnerApplicationContext.getZkJobRunnerConfig().getJobsRootDirectory() + File.separator
                        + jobUUID+File.separator+"logs" +File.separator;
                File errLogFile = FileSystemUtils.createNewFile(logDir+JobExecutor.class.getName().toLowerCase()+".err");
                processBuilder.redirectError(errLogFile);
                logger.info("stderr Log file : "+errLogFile.getAbsolutePath());
                File outLogFile = FileSystemUtils.createNewFile(logDir+JobExecutor.class.getName().toLowerCase()+".out");
                processBuilder.redirectOutput(outLogFile);
                logger.info("stdout Log file : "+outLogFile.getAbsolutePath());
                processBuilder.start();

            }
        }
    }

    /**
     * Validates the arguements
     * @param args
     */
    private static void validateArguments(String[] args) {
        if (args.length < 1) {
            System.out.println("Missing {zookeeper xml configuration} file path arguments.");
            System.exit(1);
        }
    }
}
