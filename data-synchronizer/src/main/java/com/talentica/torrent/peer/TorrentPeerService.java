package com.talentica.torrent.peer;

import java.io.File;
import java.util.Observer;
import java.util.concurrent.Future;

public interface TorrentPeerService {

  /**
   * Use this method to share files from your local file system with other peers on network.
   * 
   * @param torrentFile
   * @param seedFilesDirectory
   * @param host
   * @return {@link Future} which client can use to make operation blocking by calling get() on it.
   */
  public Future<Runnable> seedFile(byte[] torrentFile, File seedFilesDirectory, String host);

  /**
   * Use this method to share files from your local file system with other peers on network.
   * 
   * @param torrentFile
   * @param seedFilesDirectory
   * @param host
   * @param observer - To get events of seed operation's progress.
   * @return {@link Future} which client can use to make operation blocking by calling get() on it.
   */
  public Future<Runnable> seedFile(byte[] torrentFile, File seedFilesDirectory, String host,
      Observer observer);

  /**
   * Downloads file to download directory.
   * 
   * @param torrentFile
   * @param downloadDirectory
   * @param observer - To listen download progress events.
   * @return {@link Future} which client can use to make operation blocking by calling get() on it.
   */
  public Future<Runnable> downloadFile(byte[] torrentFile, File downloadDirectory,
      Observer observer);

  /**
   * Downloads file to download directory.
   * 
   * @param torrentFile
   * @param downloadDirectory
   * @return {@link Future} which client can use to make operation blocking by calling get() on it.
   */
  public Future<Runnable> downloadFile(byte[] torrentFile, File downloadDirectory);

}
