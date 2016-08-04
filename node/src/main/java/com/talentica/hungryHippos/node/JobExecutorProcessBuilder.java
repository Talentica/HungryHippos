package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.node.job.JobStatusNodeCoordinator;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * Created by rajkishoreh on 1/8/16.
 */
public class JobExecutorProcessBuilder {
    public static void main(String[] args) throws InterruptedException, IOException, JAXBException {
        validateArguments(args);
        String clientConfigPath = args[0];
        NodesManager manager = NodesManagerContext.getNodesManagerInstance(clientConfigPath);
        int nodeId = NodeInfo.INSTANCE.getIdentifier();
        while(true){
            List<String> jobUUIDs = JobStatusNodeCoordinator.checkNodeJobUUIDs(nodeId);
            for(String jobUUID:jobUUIDs){
                ProcessBuilder processBuilder = new ProcessBuilder("java",JobExecutor.class.getName(),clientConfigPath,jobUUID);
                Process process = processBuilder.start();
                int status =  process.waitFor();
                if(status!=0){
                   JobStatusNodeCoordinator.updateNodeJobFailed(jobUUID,nodeId);
                }
            }
        }
    }

    private static void validateArguments(String[] args) {
        if (args.length < 1) {
            System.out.println("Missing {zookeeper xml configuration} file path arguments.");
            System.exit(1);
        }
    }
}
