package com.talentica.hungryHippos.job.util;

import com.talentica.hungryHippos.common.context.JobRunnerApplicationContext;
import com.talentica.hungryHippos.coordination.DataSyncCoordinator;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.RandomNodePicker;
import com.talentica.hungryHippos.tools.FileSynchronizer;
import com.talentica.hungryHippos.tools.utils.RandomNodePicker;
import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by rajkishoreh on 2/8/16.
 */
public class JobJarPublisher {

    private static final Logger logger = LoggerFactory.getLogger(JobJarPublisher.class);

    /**
     * Sends the Jar file to each HH node
     * @param jobUUID
     * @param localJarPath
     * @return
     */
    public static boolean publishJar(String jobUUID, String localJarPath) {

        String userName = NodesManagerContext.getClientConfig().getOutput().getNodeSshUsername();
        Node node = RandomNodePicker.getRandomNode();
        String pathToJobRootDir = JobRunnerApplicationContext.getZkJobRunnerConfig().getJarRootDirectory();
        String remoteDir = pathToJobRootDir + File.separatorChar + jobUUID + File.separatorChar;
        File jarFile = new File(localJarPath);
        String remoteJarPath = remoteDir+jarFile.getName();
        ScpCommandExecutor.upload(userName, node.getIp(), remoteDir, localJarPath);
        try {
            DataSyncCoordinator.notifyFileSync(node.getIp(),remoteJarPath);
            return DataSyncCoordinator.checkSyncUpStatus(remoteJarPath);
        } catch (Exception e) {
            logger.error(e.toString());
            throw new RuntimeException(e);
        }
    }

}
