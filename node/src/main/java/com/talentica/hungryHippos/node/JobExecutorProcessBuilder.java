package com.talentica.hungryHippos.node;

import static com.talentica.hungryHippos.common.job.JobStatusCommonOperations.getPendingHHNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.node.job.JobStatusNodeCoordinator;

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
                ProcessBuilder processBuilder = new ProcessBuilder("java",JobExecutor.class.getName(),clientConfigPath,jobUUID);
                Process process = processBuilder.start();
                int status =  process.waitFor();
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String line = null;
                StringBuilder sb = new StringBuilder();
                while ((line = br.readLine()) != null){
                    sb.append(line).append("\n");
                }
                System.out.println("Console OUTPUT : \n"+sb.toString());
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
