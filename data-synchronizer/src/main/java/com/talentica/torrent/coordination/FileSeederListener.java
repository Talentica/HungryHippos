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

public class FileSeederListener implements PathChildrenCacheListener {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSeederListener.class);

  private static final String SEED_FILES_PARENT_NODE_PATH = "/torrent/files-to-seed/";

  private static final TorrentPeerService TORRENT_PEER_SERVICE = new TorrentPeerServiceImpl();

  private FileSeederListener() {}

  @SuppressWarnings("resource")
  public static void register(CuratorFramework client, String host) {
    PathChildrenCache childrenCache = null;
    try {
      String zkNodeToListenTo = SEED_FILES_PARENT_NODE_PATH + host;
      createListenerNodeIfDoesntExist(client, zkNodeToListenTo);
      childrenCache = new PathChildrenCache(client, zkNodeToListenTo, true);
      childrenCache.getListenable().addListener(new FileSeederListener());
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
        TORRENT_PEER_SERVICE.seedFile(torrentFile, new File(fileMetadata.getPath()),
            fileMetadata.getOriginHost());
        LOGGER.info("New children added for seeding file is added. Path is {}", path);
      }
    } catch (Exception e) {
      LOGGER.error("Error occurred while processing request to seed file for node with path: {}",
          path);
    }
  }


}
