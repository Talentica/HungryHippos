package com.talentica.hungryHippos.node.tools.handlers;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.utility.SocketMessages;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.coordination.FileDownloaderListener;
import com.talentica.torrent.coordination.NewTorrentAvailableListener;
import com.talentica.torrent.util.TorrentGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * This is a handler class for File sync up request
 * Created by rajkishoreh on 18/8/16.
 */
public class FileSyncUpRequestHandler implements RequestHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSyncUpRequestHandler.class);

    private String seedFilePath;

    private ObjectMapper objectMapper;

    private CuratorFramework client;

    public FileSyncUpRequestHandler() {
        this.objectMapper = new ObjectMapper();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
        String connectString = NodesManagerContext.getClientConfig().getCoordinationServers().getServers();
        client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        client.start();
    }

    private void startSync() throws Exception {
        String originHost = NodeInfo.INSTANCE.getIp();
        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setOriginHost(originHost);
        fileMetadata.setPath(seedFilePath);
        File torrentFile = TorrentGenerator.generateTorrentFile(client,
                new File(seedFilePath));
        fileMetadata.setBase64EncodedTorrentFile(
                DatatypeConverter.printBase64Binary(FileUtils.readFileToByteArray(torrentFile)));
        client.create().creatingParentsIfNeeded().forPath(
                NewTorrentAvailableListener.NEW_TORRENT_AVAILABLE_NODE_PATH + "/"
                        + System.currentTimeMillis(),
                objectMapper.writeValueAsString(fileMetadata).getBytes());
    }

    private boolean checkSyncUpStatus() throws Exception {
        List<Node> nodeList = CoordinationApplicationContext.getLocalClusterConfig().getNode();
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
                    return true;
                }
            }

            Stat failureNodeStat = client.checkExists().forPath(nodeForCheckingFailure);
            if (failureNodeStat != null) {
                int numFailureNodes = failureNodeStat.getNumChildren();
                if (numFailureNodes > 0) {
                    return false;
                }
            }
        }
    }

    @Override
    public void handleRequest(DataInputStream dis, DataOutputStream dos) throws IOException {
        LOGGER.info("[{}] Request handling started",Thread.currentThread().getName());
        try {
            seedFilePath = dis.readUTF();
            startSync();
            boolean status = checkSyncUpStatus();
            if (status) {
                LOGGER.info("[{}] File sync up successful for : {}",Thread.currentThread().getName(),seedFilePath);
                dos.writeInt(SocketMessages.SUCCESS.ordinal());
            } else {
                LOGGER.info("[{}] File sync up failed for : {}",Thread.currentThread().getName(),seedFilePath);
                dos.writeInt(SocketMessages.FAILED.ordinal());
            }
        } catch (Exception e) {
            dos.writeInt(SocketMessages.FAILED.ordinal());
            LOGGER.error("[{}] {}", Thread.currentThread().getName(), e.toString());
            e.printStackTrace();
        }
    }
}
