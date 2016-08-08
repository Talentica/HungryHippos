package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.node.job.JobStatusNodeCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

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
        NodesManager manager = NodesManagerContext.getNodesManagerInstance(clientConfigPath);
        int nodeId = NodeInfo.INSTANCE.getIdentifier();
        while(true){
            List<String> jobUUIDs = JobStatusNodeCoordinator.checkNodeJobUUIDs(nodeId);
            if(jobUUIDs==null||jobUUIDs.size()==0){
                continue;
            }
            for(String jobUUID:jobUUIDs){
                logger.info("Processing "+jobUUID);
                ProcessBuilder processBuilder = new ProcessBuilder("java",JobExecutor.class.getName(),clientConfigPath,jobUUID);
                Process process = processBuilder.start();
                int status =  process.waitFor();
                if(status!=0){
                   JobStatusNodeCoordinator.updateNodeJobFailed(jobUUID,nodeId);
                }
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
