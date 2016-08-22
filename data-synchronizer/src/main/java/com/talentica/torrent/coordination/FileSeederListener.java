package com.talentica.torrent.coordination;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.DatatypeConverter;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.DataSynchronizerStarter;
import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.peer.TorrentPeerService;
import com.talentica.torrent.peer.TorrentPeerServiceImpl;
import com.talentica.torrent.util.Environment;

public class FileSeederListener extends ChildrenUpdatedListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSeederListener.class);

  public static final String SEED_FILES_PARENT_NODE_PATH =
      Environment.getPropertyValue("files.to.seed.node.path");

  public static final String FILES_ERRED_WHILE_SEEDING_NODE_PATH =
      Environment.getPropertyValue("files.erred.while.seeding.node.path");

  private static final TorrentPeerService TORRENT_PEER_SERVICE = new TorrentPeerServiceImpl();

  private static String zkNodeToListenTo;

  private static String host;

  private FileSeederListener() {}

  public static void register(CuratorFramework client, String thisHost) {
    host = thisHost;
    zkNodeToListenTo = SEED_FILES_PARENT_NODE_PATH + thisHost;
    register(client, zkNodeToListenTo, new FileSeederListener());
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
    String path = null;
    try {
      ChildData childData = event.getData();
      if (childData != null) {
        path = childData.getPath();
        if (checkIfNodeAddedOrUpdated(event)) {
          byte[] metadataAboutFileToSeed = childData.getData();
          FileMetadata fileMetadata =
              OBJECT_MAPPER.readValue(metadataAboutFileToSeed, FileMetadata.class);
          File fileToSeed = new File(fileMetadata.getPath());
          byte[] torrentFileContent =
              DatatypeConverter.parseBase64Binary(fileMetadata.getBase64EncodedTorrentFile());
          String originHost = fileMetadata.getOriginHost();
          TORRENT_PEER_SERVICE.seedFile(torrentFileContent, fileToSeed.getParentFile(), originHost)
              .get();
          readyForDownload(client, fileMetadata);
        }
      }
    } catch (Exception exception) {
      handleError(client, path, exception, FILES_ERRED_WHILE_SEEDING_NODE_PATH);
    } finally {
      cleanup(client, path);
    }
  }

  private void readyForDownload(CuratorFramework client, FileMetadata fileMetadata)
      throws IOException, JsonGenerationException, JsonMappingException, Exception {
    String fileMetadataWithTorrent = OBJECT_MAPPER.writeValueAsString(fileMetadata);
    client.getChildren().forPath(DataSynchronizerStarter.TORRENT_PEERS_NODE_PATH).stream()
        .filter(childPath -> !host.equals(childPath.split("/")[childPath.split("/").length - 1]))
        .forEach(childPath -> {
          try {
            createDownloadReadyNodesForOtherPeers(client, fileMetadataWithTorrent,
                childPath.split("/")[childPath.split("/").length - 1]);
          } catch (Exception exception) {
            LOGGER.error("Error occurred while creating download ready for node: {}", childPath,
                exception);
          }
        });
  }

  private void createDownloadReadyNodesForOtherPeers(CuratorFramework client,
      String fileMetadataWithTorrent, String downloadHost) throws Exception {
    String fileAvailableForDownloadBasePath = FileDownloaderListener.FILES_TO_DOWNLOAD_NODE_PATH
        + downloadHost + "/" + System.currentTimeMillis();
    try {
      if (client.checkExists().forPath(fileAvailableForDownloadBasePath) == null) {
        client.create().creatingParentsIfNeeded().forPath(fileAvailableForDownloadBasePath,
            fileMetadataWithTorrent.getBytes());
      } else {
        client.setData().forPath(fileAvailableForDownloadBasePath,
            fileMetadataWithTorrent.getBytes());
      }
    } catch (NodeExistsException exception) {
      LOGGER.warn("Node already exists: {}", fileAvailableForDownloadBasePath);
    }
  }

}
