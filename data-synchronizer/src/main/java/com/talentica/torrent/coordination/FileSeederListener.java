package com.talentica.torrent.coordination;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.DataSynchronizerStarter;
import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.TorrentTrackerStarter;
import com.talentica.torrent.peer.TorrentPeerService;
import com.talentica.torrent.peer.TorrentPeerServiceImpl;
import com.talentica.torrent.tracker.TorrentTrackerService;
import com.talentica.torrent.tracker.TorrentTrackerServiceImpl;
import com.talentica.torrent.util.TorrentGenerator;

public class FileSeederListener implements PathChildrenCacheListener {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSeederListener.class);

  public static final String SEED_FILES_PARENT_NODE_PATH = "/torrent/files-to-seed/";

  private static final TorrentPeerService TORRENT_PEER_SERVICE = new TorrentPeerServiceImpl();

  private static final TorrentTrackerService TORRENT_TRACKER_SERVICE =
      new TorrentTrackerServiceImpl();

  private static String zkNodeToListenTo;

  private FileSeederListener() {}

  @SuppressWarnings("resource")
  public static void register(CuratorFramework client, String host) {
    PathChildrenCache childrenCache = null;
    try {
      zkNodeToListenTo = SEED_FILES_PARENT_NODE_PATH + host;
      createListenerNodeIfDoesntExist(client, zkNodeToListenTo);
      childrenCache = new PathChildrenCache(client, zkNodeToListenTo, true);
      childrenCache.getListenable().addListener(new FileSeederListener());
      childrenCache.start(StartMode.POST_INITIALIZED_EVENT);
      LOGGER.info("FileSeederListener registered successfully.");
    } catch (Exception exception) {
      LOGGER.error("Error occurred while registering FileSeederListener.", exception);
      closeChildrenCache(childrenCache);
      throw new RuntimeException(exception);
    }
  }

  private static void createListenerNodeIfDoesntExist(CuratorFramework client,
      String zkNodeToListenTo) throws Exception {
    boolean pathDoesntExist = client.checkExists().forPath(zkNodeToListenTo) == null;
    if (pathDoesntExist) {
      client.create().creatingParentsIfNeeded().forPath(zkNodeToListenTo);
    }
  }

  private static void closeChildrenCache(PathChildrenCache childrenCache) {
    try {
      if (childrenCache != null) {
        childrenCache.close();
      }
    } catch (Exception exception) {
      LOGGER.error("Error occurred while closing path children cache.", exception);
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
    String path = "";
    try {
      ChildData childData = event.getData();
      if (childData != null) {
        path = childData.getPath();
        if (event.getType() == Type.CHILD_ADDED || event.getType() == Type.CHILD_UPDATED) {
          byte[] metadataAboutFileToSeed = childData.getData();
          FileMetadata fileMetadata =
              OBJECT_MAPPER.readValue(metadataAboutFileToSeed, FileMetadata.class);
          File fileToSeed = new File(fileMetadata.getPath());
          File torrentFile = generateTorrentFile(client, fileToSeed);
          TORRENT_TRACKER_SERVICE.newTorrentFileAvailable(torrentFile);
          byte[] torrentFileContent = FileUtils.readFileToByteArray(torrentFile);
          String originHost = fileMetadata.getOriginHost();
          TORRENT_PEER_SERVICE.seedFile(torrentFileContent, fileToSeed, originHost);
          fileMetadata
              .setBase64EncodedTorrentFile(DatatypeConverter.printBase64Binary(torrentFileContent));
          String fileMetadataWithTorrent = OBJECT_MAPPER.writeValueAsString(fileMetadata);

          client.getChildren().forPath(DataSynchronizerStarter.TORRENT_PEERS_NODE_PATH)
              .forEach(childPath -> {
                try {
                  createDownloadReadyNodesForOtherPeers(client, 
                      fileMetadataWithTorrent,
                      childPath.split("/")[childPath.split("/").length - 1]);
                } catch (Exception exception) {
                  LOGGER.error("Error occurred while creating download ready for node: {}",
                      childPath, exception);
                }
              });
          client.delete().forPath(path);
        }
      }
    } catch (Exception e) {
      LOGGER.error(
          "Error occurred while processing request to seed file for node with path: " + path, e);
    }
  }

  private void createDownloadReadyNodesForOtherPeers(CuratorFramework client,
      String fileMetadataWithTorrent, String downloadHost)
      throws Exception {
    String fileAvailableNodePath = FileDownloaderListener.FILES_TO_DOWNLOAD_NODE_PATH + downloadHost
        + "/" + System.currentTimeMillis();
    if (client.checkExists().forPath(fileAvailableNodePath) == null) {
      client.create().creatingParentsIfNeeded().forPath(fileAvailableNodePath,
          fileMetadataWithTorrent.getBytes());
    } else {
      client.setData().forPath(fileAvailableNodePath, fileMetadataWithTorrent.getBytes());
    }
  }

  private File generateTorrentFile(CuratorFramework client, File seedFilesDirectory)
      throws Exception, IOException {
    List<URI> announceList = new ArrayList<>(1);
    client.getChildren().forPath(TorrentTrackerStarter.TRACKERS_NODE_PATH).forEach(childPath -> {
      try {
        announceList.add(URI.create(new String(
            client.getData().forPath(TorrentTrackerStarter.TRACKERS_NODE_PATH + "/" + childPath))));
      } catch (Exception exception) {
        LOGGER.error("Error occurred while creating trackers announce list for path: {}", childPath,
            exception);
      }
    });
    File torrentFile = File.createTempFile(seedFilesDirectory.getName(), ".torrent");
    TorrentGenerator.generateTorrentFile(seedFilesDirectory.getAbsolutePath(), announceList,
        "SYSTEM", torrentFile.getAbsolutePath());
    return torrentFile;
  }

}
