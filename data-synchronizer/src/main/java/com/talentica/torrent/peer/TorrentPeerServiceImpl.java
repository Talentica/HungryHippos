package com.talentica.torrent.peer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.util.Environment;
import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;

public class TorrentPeerServiceImpl implements TorrentPeerService {

  private static final int THREAD_POOL_SIZE =
      Environment.getPropertyValueAsInteger("peer.service.fixed.thread.pool.size");

  private static final Logger LOGGER = LoggerFactory.getLogger(TorrentPeerServiceImpl.class);

  private static final ExecutorService EXECUTOR_SERVICE =
      Executors.newFixedThreadPool(THREAD_POOL_SIZE);

  @Override
  public Future<Runnable> seedFile(byte[] torrentFile, File seedFilesDirectory, String host) {
    return seedFile(torrentFile, seedFilesDirectory, host, new Observer() {
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
  public Future<Runnable> seedFile(byte[] torrentFile, File seedFilesDirectory, String hostName,
      Observer observer) {
    SeedFileTask seedFileTask =
        new SeedFileTask(torrentFile, seedFilesDirectory, observer, hostName);
    return EXECUTOR_SERVICE.submit(seedFileTask, seedFileTask);
  }

  private class SeedFileTask implements Runnable {

    private byte[] torrentFile;

    private File seedFilesDirectory;

    private Observer observer;

    private String hostName;

    SeedFileTask(byte[] torrentFile, File seedFilesDirectory, Observer observer, String host) {
      this.torrentFile = torrentFile;
      this.seedFilesDirectory = seedFilesDirectory;
      this.observer = observer;
      this.hostName = host;
    }

    @Override
    public void run() {
      try {
        SharedTorrent sharedTorrent = new SharedTorrent(torrentFile, seedFilesDirectory, true);
        InetAddress host = InetAddress.getByName(hostName);
        Client client = new Client(host, sharedTorrent);
        client.addObserver(observer);
        client.share();
        LOGGER.info("Seeding file:" + seedFilesDirectory + " is successful.");
      } catch (IOException | NoSuchAlgorithmException exception) {
        LOGGER.warn("Seeding file " + seedFilesDirectory + " failed", exception.getMessage());
      }
    }
  }


  @Override
  public Future<Runnable> downloadFile(byte[] torrentFile, File downloadDirectory) {
    return downloadFile(torrentFile, downloadDirectory, new Observer() {
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

  @Override
  public Future<Runnable> downloadFile(byte[] torrentFile, File downloadDirectory,
      Observer observer) {
    DownloadFileTask downloadTask = new DownloadFileTask(torrentFile, downloadDirectory, observer);
    return EXECUTOR_SERVICE.submit(downloadTask, downloadTask);
  }

  private class DownloadFileTask implements Runnable {

    private byte[] torrentFile;

    private File downloadDirectory;

    private Observer observer;

    DownloadFileTask(byte[] torrentFile, File fileToDownload, Observer observer) {
      if (fileToDownload.getParentFile() != null) {
        this.downloadDirectory = fileToDownload.getParentFile();
      } else {
        this.downloadDirectory = fileToDownload;
      }
      this.downloadDirectory.mkdirs();
      this.torrentFile = torrentFile;
      this.observer = observer;
    }

    @Override
    public void run() {
      Client client = null;
      try {
        client = new Client(InetAddress.getLocalHost(),
            new SharedTorrent(torrentFile, downloadDirectory));
        client.addObserver(observer);
        client.download();
        client.waitForCompletion();
      } catch (NoSuchAlgorithmException | IOException exception) {
        throw new RuntimeException(exception);
      } finally {
        if (client != null) {
          client.stop();
        }
      }
    }
  }

}
