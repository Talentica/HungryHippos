package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by rajkishoreh on 1/8/16.
 */
public class JobProcessBuilder {
    public static void main(String[] args) throws InterruptedException, IOException, JAXBException {
        validateArguments(args);
        String clientConfigPath = args[0];
        NodesManager manager = NodesManagerContext.getNodesManagerInstance(clientConfigPath);
        while(true){
            //TODO Listen to node for pending jobs and return jobUUID
            String jobUUID = "";
            ProcessBuilder processBuilder = new ProcessBuilder("java",JobExecutor.class.getName(),clientConfigPath,jobUUID);
            Process process = processBuilder.start();
            int status =  process.waitFor();
            if(status!=0){
                //TODO Error handling
            }
        }

    }

    private static void validateArguments(String[] args) {
        if (args.length < 1) {
            System.out
                    .println("Missing {zookeeper xml configuration} file path arguments.");
            System.exit(1);
        }
    }
}
