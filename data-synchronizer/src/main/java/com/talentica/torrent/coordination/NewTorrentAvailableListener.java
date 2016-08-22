package com.talentica.torrent.coordination;

import java.io.File;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.tracker.TorrentTrackerServiceImpl;

public class NewTorrentAvailableListener extends ChildrenUpdatedListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(NewTorrentAvailableListener.class);

  public static final String NEW_TORRENT_AVAILABLE_NODE_PATH = "/torrent/new-torrent-available";

  public static final String NEW_TORRENT_AVAILABLE_ERROR_NODE_PATH =
      "/torrent/new-torrent-available-erred-requests/";

  private NewTorrentAvailableListener() {}

  public static void register(CuratorFramework client, String thisHost) {
    register(client, NEW_TORRENT_AVAILABLE_NODE_PATH, new NewTorrentAvailableListener());
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    String path = null;
    try {
      ChildData childData = event.getData();
      if (childData != null) {
        path = childData.getPath();
        if (checkIfNodeAddedOrUpdated(event)) {
          LOGGER.info("Processing new torrent file received at: " + path);
          byte[] metadataAboutFileToSeed = childData.getData();
          FileMetadata fileMetadata =
              OBJECT_MAPPER.readValue(metadataAboutFileToSeed, FileMetadata.class);
          byte[] torrentFileBytes =
              DatatypeConverter.parseBase64Binary(fileMetadata.getBase64EncodedTorrentFile());
          File torrentFile = File.createTempFile("" + System.currentTimeMillis(), ".torrent");
          FileUtils.writeByteArrayToFile(torrentFile, torrentFileBytes);
          TorrentTrackerServiceImpl.getInstance().announeNewTorrent(torrentFile);
          client.create().creatingParentsIfNeeded().forPath(
              FileSeederListener.SEED_FILES_PARENT_NODE_PATH + fileMetadata.getOriginHost() + "/"
                  + System.currentTimeMillis(),
              OBJECT_MAPPER.writeValueAsString(fileMetadata).getBytes());
          LOGGER.info("New torrent file available with tracker for:" + path);
        }
      }
    } catch (Exception exception) {
      handleError(client, path, exception, NEW_TORRENT_AVAILABLE_ERROR_NODE_PATH);
    } finally {
      cleanup(client, path);
    }
  }

}
