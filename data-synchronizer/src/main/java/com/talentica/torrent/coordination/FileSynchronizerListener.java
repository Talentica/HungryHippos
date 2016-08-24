package com.talentica.torrent.coordination;

import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.util.Environment;
import com.talentica.torrent.util.TorrentGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.codehaus.jackson.map.ObjectMapper;

import javax.xml.bind.DatatypeConverter;
import java.io.File;

/**
 * Created by rajkishoreh on 22/8/16.
 */
public class FileSynchronizerListener extends ChildrenUpdatedListener {

    public static final String FILES_FOR_SYNC = 
        Environment.getPropertyValue("files.for.sync");
    public static final String FILE_SYNC_FAILURES = 
        Environment.getPropertyValue("file.sync.failures");

    private static String host;

    public static void register(CuratorFramework client,String host) {
        FileSynchronizerListener.host = host;
        String zkNodeToListenTo = FILES_FOR_SYNC + host;
        register(client, zkNodeToListenTo, new FileSynchronizerListener());
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        String path = null;
        try {
            ChildData childData = event.getData();
            if (childData != null) {
                path = childData.getPath();
                if (checkIfNodeAddedOrUpdated(event)) {
                    updateMetaData(client,path);
                }
            }
        } catch (Exception exception) {
            handleError(client, path, exception, FILE_SYNC_FAILURES);
        } finally {
            cleanup(client, path);
        }
    }


    private void updateMetaData(CuratorFramework client, String path) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] dataArray = client.getData().forPath(path);
        String seedFilePath = new String(dataArray);
        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setOriginHost(host);
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
}
