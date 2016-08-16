package com.talentica.torrent.coordination;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.DataSynchronizerStarter;
import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.TorrentTrackerStarter;
import com.talentica.torrent.peer.TorrentPeerService;
import com.talentica.torrent.peer.TorrentPeerServiceImpl;
import com.talentica.torrent.tracker.TorrentTrackerServiceImpl;
import com.talentica.torrent.util.TorrentGenerator;

public class FileSeederListener implements PathChildrenCacheListener {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSeederListener.class);

  public static final String SEED_FILES_PARENT_NODE_PATH = "/torrent/files-to-seed/";

  public static final String FILES_ERRED_WHILE_SEEDING_NODE_PATH =
      "/torrent/files-erred-while-seeding/";

  private static final TorrentPeerService TORRENT_PEER_SERVICE = new TorrentPeerServiceImpl();

  private static String zkNodeToListenTo;

  private static String host;

  private FileSeederListener() {}

  @SuppressWarnings("resource")
  public static void register(CuratorFramework client, String thisHost) {
    PathChildrenCache childrenCache = null;
    try {
      zkNodeToListenTo = SEED_FILES_PARENT_NODE_PATH + thisHost;
      createListenerNodeIfDoesntExist(client, zkNodeToListenTo);
      childrenCache = new PathChildrenCache(client, zkNodeToListenTo, true);
      childrenCache.getListenable().addListener(new FileSeederListener());
      childrenCache.start(StartMode.POST_INITIALIZED_EVENT);
      host = thisHost;
      LOGGER.info("FileSeederListener registered successfully.");
    } catch (Exception exception) {
      LOGGER.error("Error occurred while registering FileSeederListener.", exception);
      closeChildrenCache(childrenCache);
      throw new RuntimeException(exception);
    }
  }

  private static void createListenerNodeIfDoesntExist(CuratorFramework client,
      String zkNodeToListenTo) throws Exception {
    try {
      boolean pathDoesntExist = client.checkExists().forPath(zkNodeToListenTo) == null;
      if (pathDoesntExist) {
        client.create().creatingParentsIfNeeded().forPath(zkNodeToListenTo);
      }
    } catch (NodeExistsException exception) {
      LOGGER.warn("Node already exists: {}", zkNodeToListenTo);
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
          File torrentFile = generateTorrentFile(client, fileToSeed);
          TorrentTrackerServiceImpl.getInstance().newTorrentFileAvailable(torrentFile);
          byte[] torrentFileContent = FileUtils.readFileToByteArray(torrentFile);
          String originHost = fileMetadata.getOriginHost();
          TORRENT_PEER_SERVICE.seedFile(torrentFileContent, fileToSeed.getParentFile(), originHost)
              .get();
          fileMetadata
              .setBase64EncodedTorrentFile(DatatypeConverter.printBase64Binary(torrentFileContent));
          readyForDownload(client, fileMetadata);
        }
      }
    } catch (Exception exception) {
      handleError(client, path, exception);
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

  private boolean checkIfNodeAddedOrUpdated(PathChildrenCacheEvent event) {
    return event.getType() == Type.CHILD_ADDED || event.getType() == Type.CHILD_UPDATED;
  }

  private void cleanup(CuratorFramework client, String path) {
    try {
      if (path != null && client.checkExists().forPath(path) != null) {
        client.delete().forPath(path);
      }
    } catch (Exception exception) {
      LOGGER.error("Error occurred while deleting node:" + path, exception);
    }
  }

  private void handleError(CuratorFramework client, String path, Exception error) {
    try {
      LOGGER.error(
          "Error occurred while processing request to seed file for node with path: " + path,
          error);
      if (path != null) {
        client.create().creatingParentsIfNeeded().forPath(
            FILES_ERRED_WHILE_SEEDING_NODE_PATH + StringUtils.substringAfterLast(path, "/"));
      }
    } catch (Exception exception) {
      LOGGER.error("Error occurred while deleting node:" + path, exception);
    }
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

  private File generateTorrentFile(CuratorFramework client, File seedFile)
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
    File torrentFile = File.createTempFile(seedFile.getName(), ".torrent");
    TorrentGenerator.generateTorrentFile(seedFile.getAbsolutePath(), announceList, "SYSTEM",
        torrentFile.getAbsolutePath());
    return torrentFile;
  }

}
