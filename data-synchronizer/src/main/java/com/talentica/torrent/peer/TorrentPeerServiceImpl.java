package com.talentica.torrent.peer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Observable;
import java.util.Observer;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;

public class TorrentPeerServiceImpl implements TorrentPeerService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TorrentPeerServiceImpl.class);

  @Override
  public void seedFile(File torrentFile, File seedFilesDirectory, String hostName,
      Observer observer) {
    try {
      SharedTorrent sharedTorrent =
          new SharedTorrent(FileUtils.readFileToByteArray(torrentFile), seedFilesDirectory, true);
      InetAddress host = InetAddress.getByName(hostName);
      Client client = new Client(host, sharedTorrent);
      client.addObserver(observer);
      client.share();
      client.waitForCompletion();
    } catch (IOException | NoSuchAlgorithmException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void seedFile(File torrentFile, File seedFilesDirectory, String host) {
    seedFile(torrentFile, seedFilesDirectory, host, new Observer() {
      @Override
      public void update(Observable observable, Object data) {
        Client client = (Client) observable;
        SharedTorrent torrent = client.getTorrent();
        float progress = torrent.getCompletion();
        LOGGER.debug("{} % uploaded for torrent: {}", new Object[] {progress, torrent.getName()});
      }
    });
  }

  @Override
  public void downloadFile(File torrentFile, File downloadDirectory, Observer observer) {
    Client client = null;
    try {
      client = new Client(InetAddress.getLocalHost(),
          SharedTorrent.fromFile(torrentFile, downloadDirectory));
      client.download();
      client.addObserver(observer);
      client.waitForCompletion();
    } catch (NoSuchAlgorithmException | IOException exception) {
      throw new RuntimeException(exception);
    } finally {
      if (client != null) {
        client.stop();
      }
    }
  }

  @Override
  public void downloadFile(File torrentFile, File downloadDirectory) {
    downloadFile(torrentFile, downloadDirectory, new Observer() {
      @Override
      public void update(Observable observable, Object data) {
        Client client = (Client) observable;
        SharedTorrent torrent = client.getTorrent();
        float progress = torrent.getCompletion();
        LOGGER.debug("{} % files downloaded for torrent: {}",
            new Object[] {progress, torrent.getName()});
      }
    });
  }

}
