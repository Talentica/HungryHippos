package com.talentica.torrent.tracker;

import java.io.File;

public interface TorrentTrackerService {

  /**
   * Starts a new tracker on host specified at the specified port. Throws error if tracker was
   * already started on specified port earlier.
   * 
   * @param port
   */
  public void startTracker(String host, int port);

  /**
   * Updates tracker on specified port on localhost about new available torrent file.
   * 
   * @param torrentFile
   */
  public void newTorrentFileAvailable(File torrentFile);

  /**
   * Stops tracker on specified port on localhost.
   * 
   * @param port
   */
  public void stopTracker();

  public boolean isTorrentAvailableForFileName(String filename);

  boolean isTrackerStarted();

}
