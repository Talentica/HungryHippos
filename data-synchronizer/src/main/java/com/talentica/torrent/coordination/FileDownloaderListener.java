package com.talentica.torrent.coordination;

import java.io.File;

import javax.xml.bind.DatatypeConverter;

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

import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.peer.TorrentPeerService;
import com.talentica.torrent.peer.TorrentPeerServiceImpl;

public class FileDownloaderListener implements PathChildrenCacheListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileDownloaderListener.class);

  private static final String DOWNLOAD_FILES_PARENT_NODE_PATH = "/torrent/files-to-download/";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final TorrentPeerService TORRENT_PEER_SERVICE = new TorrentPeerServiceImpl();

  private static String host = null;

  private FileDownloaderListener() {}

  @SuppressWarnings("resource")
  public static void register(CuratorFramework client, String thisHost) {
    PathChildrenCache childrenCache = null;
    try {
      host = thisHost;
      String zkNodeToListenTo = DOWNLOAD_FILES_PARENT_NODE_PATH + host;
      createListenerNodeIfDoesntExist(client, zkNodeToListenTo);
      childrenCache = new PathChildrenCache(client, zkNodeToListenTo, true);
      childrenCache.getListenable().addListener(new FileDownloaderListener());
      childrenCache.start(StartMode.BUILD_INITIAL_CACHE);
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
      client.create().forPath(zkNodeToListenTo);
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
      path = childData.getPath();
      if (event.getType() == Type.CHILD_ADDED) {
        byte[] metadataAboutFileToSeed = childData.getData();
        FileMetadata fileMetadata =
            OBJECT_MAPPER.readValue(metadataAboutFileToSeed, FileMetadata.class);
        byte[] torrentFile =
            DatatypeConverter.parseBase64Binary(fileMetadata.getBase64EncodedTorrentFile());
        TORRENT_PEER_SERVICE.downloadFile(torrentFile, new File(fileMetadata.getPath()));
        LOGGER.info("New children added for downloading file is added. Path is {}", path);
      }
    } catch (Exception e) {
      LOGGER.error("Error occurred while processing request to download file on node with path: {}",
          new Object[] {host, path});
    }
  }

}
