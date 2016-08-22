package com.talentica.torrent.coordination;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutionException;

import javax.xml.bind.DatatypeConverter;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.peer.TorrentPeerService;
import com.talentica.torrent.peer.TorrentPeerServiceImpl;
import com.talentica.torrent.util.Environment;
import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;

public class FileDownloaderListener extends ChildrenUpdatedListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileDownloaderListener.class);

  public static final String FILES_TO_DOWNLOAD_NODE_PATH =
      Environment.getPropertyValue("files.to.download.node.path");

  public static final String FILES_DOWNLOAD_SUCCESS_NODE_PATH =
      Environment.getPropertyValue("files.download.success.node.path");

  public static final String FILES_ERRED_WHILE_DOWNLOAD_NODE_PATH =
      Environment.getPropertyValue("files.erred.while.download.node.path");

  private static final TorrentPeerService TORRENT_PEER_SERVICE = new TorrentPeerServiceImpl();

  private static String host = null;

  private FileDownloaderListener() {}


  public static void register(CuratorFramework client, String thisHost) {
    host = thisHost;
    String zkNodeToListenTo = FILES_TO_DOWNLOAD_NODE_PATH + host;
    register(client, zkNodeToListenTo, new FileDownloaderListener());
  }

  @Override
  public void childEvent(CuratorFramework curatorClient, PathChildrenCacheEvent event) {
    String path = null;
    try {
      ChildData childData = event.getData();
      if (childData != null) {
        path = childData.getPath();
        if (checkIfNodeAddedOrUpdated(event)) {
          startDownloadProcess(curatorClient, path, childData);
        }
      }
    } catch (Exception exception) {
      handleError(curatorClient, path, exception, FILES_ERRED_WHILE_DOWNLOAD_NODE_PATH);
    } finally {
      cleanup(curatorClient, path);
    }
  }

  private void startDownloadProcess(CuratorFramework curatorClient, String path,
      ChildData childData) throws IOException, JsonParseException, JsonMappingException,
      InterruptedException, ExecutionException {
    byte[] metadataAboutFileToDownload = childData.getData();
    if (metadataAboutFileToDownload != null && metadataAboutFileToDownload.length > 0) {
      FileMetadata fileMetadata =
          OBJECT_MAPPER.readValue(metadataAboutFileToDownload, FileMetadata.class);
      byte[] torrentFile =
          DatatypeConverter.parseBase64Binary(fileMetadata.getBase64EncodedTorrentFile());
      File downloadDirectory = new File(fileMetadata.getPath());
      LOGGER.info("Downloading file with node path: {}", path);
      TORRENT_PEER_SERVICE.downloadFile(torrentFile, downloadDirectory,
          new DownloadProgressObserver(curatorClient, downloadDirectory)).get();
      LOGGER.info("Downloading finished for: {}", path);
    }
  }

  private class DownloadProgressObserver implements Observer {

    private CuratorFramework curatorClient;

    private File downloadDirectory;

    public DownloadProgressObserver(CuratorFramework client, File downloadDirectory) {
      this.downloadDirectory = downloadDirectory;
      this.curatorClient = client;
    }

    @Override
    public void update(Observable observable, Object data) {
      updateProgressOfFileDownload(curatorClient, downloadDirectory, observable);
    }

    private void updateProgressOfFileDownload(CuratorFramework curatorClient,
        File downloadDirectory, Observable observable) {
      try {
        Client client = (Client) observable;
        SharedTorrent torrent = client.getTorrent();
        float progress = torrent.getCompletion();
        LOGGER.debug("{} % file downloaded for torrent: {}",
            new Object[] {progress, torrent.getName()});
        if (Float.valueOf(progress).intValue() == 100) {
          String fileDownloadSuccessNodePath =
              FILES_DOWNLOAD_SUCCESS_NODE_PATH + downloadDirectory.getAbsolutePath() + "/" + host;
          if (curatorClient.checkExists().forPath(fileDownloadSuccessNodePath) == null) {
            curatorClient.create().creatingParentsIfNeeded().forPath(fileDownloadSuccessNodePath,
                new Date().toString().getBytes());
          } else {
            curatorClient.setData().forPath(fileDownloadSuccessNodePath,
                new Date().toString().getBytes());
          }
          LOGGER.info("File download successful for file: " + downloadDirectory.getAbsolutePath()
              + " on host:" + host);
        }
      } catch (Exception exception) {
        LOGGER.error("Error while updating progress of file download for torrent:"
            + downloadDirectory.getAbsolutePath(), exception);
      }
    }
  }

}
