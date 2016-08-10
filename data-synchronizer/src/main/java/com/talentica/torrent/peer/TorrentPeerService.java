package com.talentica.torrent.peer;

import java.io.File;
import java.util.Observer;

public interface TorrentPeerService {

  /**
   * Use this method to share files from your local file system with other peers on network.
   * 
   * @param torrentFile
   * @param seedFilesDirectory
   * @param host
   */
  public void seedFile(File torrentFile, File seedFilesDirectory, String host);

  /**
   * Use this method to share files from your local file system with other peers on network.
   * 
   * @param torrentFile
   * @param seedFilesDirectory
   * @param host
   * @param observer - To get events of seed operation's progress.
   */
  public void seedFile(File torrentFile, File seedFilesDirectory, String host, Observer observer);

  /**
   * Downloads file to download directory.
   * 
   * @param torrentFile
   * @param downloadDirectory
   * @param observer - To listen download progress events.
   */
  public void downloadFile(File torrentFile, File downloadDirectory, Observer observer);

  /**
   * Downloads file to download directory.
   * 
   * @param torrentFile
   * @param downloadDirectory
   */
  public void downloadFile(File torrentFile, File downloadDirectory);

}
