package com.talentica.hungryHippos.coordination;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.torrent.FileSynchronizerListener;
import com.talentica.torrent.coordination.FileDownloaderListener;

/**
 * This class is for coordinating with the Data-synchronizer by notifying and getting information.
 * Created by rajkishoreh on 22/8/16.
 */
public class DataSyncCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSyncCoordinator.class);


    /**
     * Returns a new client of CuratorFramework
     *
     * @return
     */
    public static CuratorFramework getCuratorFrameworkClient() {
        String connectionString = NodesManagerContext.getClientConfig().getCoordinationServers().getServers();
        CuratorFramework client;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
        client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        return client;
    }

    /**
     * Notifies Data-synchronizer for file sync up
     *
     * @param hostIP
     * @param uploadDestinationPath
     * @throws Exception
     */
    public static void notifyFileSync(String hostIP, String uploadDestinationPath) throws Exception {
        LOGGER.info("Notifying File sync up hotsIP :{} file");
        LOGGER.info("hotsIP :{} file :{}",hostIP,uploadDestinationPath);
        CuratorFramework client = getCuratorFrameworkClient();
        client.start();
        client.create().creatingParentsIfNeeded().forPath(
                FileSynchronizerListener.FILES_FOR_SYNC + "/" + hostIP + "/" +
                        +System.currentTimeMillis(), uploadDestinationPath.getBytes());
        client.close();
    }

    /**
     * Checks the sync up status of the file
     *
     * @param seedFilePath
     * @return
     * @throws Exception
     */
    public static boolean checkSyncUpStatus(String seedFilePath) throws Exception {
        LOGGER.info("Checking Sync Up Status of :{}",seedFilePath);
        CuratorFramework client = getCuratorFrameworkClient();
        List<Node> nodeList = CoordinationConfigUtil.getLocalClusterConfig().getNode();
        int totalNoOfNodes = nodeList.size();
        int requiredSuccessNodes = totalNoOfNodes - 1;
        String nodeForCheckingSuccess = FileDownloaderListener.FILES_DOWNLOAD_SUCCESS_NODE_PATH + seedFilePath;
        String nodeForCheckingFailure = FileDownloaderListener.FILES_ERRED_WHILE_DOWNLOAD_NODE_PATH
                + StringUtils.substringAfterLast(seedFilePath, "/");
        while (true) {
            Stat successNodeStat = client.checkExists().forPath(nodeForCheckingSuccess);
            if (successNodeStat != null) {
                int numSuccessNodes = successNodeStat.getNumChildren();
                if (numSuccessNodes == requiredSuccessNodes) {
                    client.close();
                    return true;
                }
            }

            Stat failureNodeStat = client.checkExists().forPath(nodeForCheckingFailure);
            if (failureNodeStat != null) {
                int numFailureNodes = failureNodeStat.getNumChildren();
                if (numFailureNodes > 0) {
                    client.close();
                    return false;
                }
            }
        }

    }
}
