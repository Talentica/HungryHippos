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
  public void announeNewTorrent(File torrentFile);

  /**
   * Stops tracker on specified port on localhost.
   * 
   * @param port
   */
  public void stopTracker();
  
 /**
  * checks whether a torrent is available for a particular file in the system.
  * @param filename
  * @return boolean true if available otherwise false.
  */
  public boolean isTorrentAvailableForFileName(String filename);

  /**
   * checks whether tracker is started or not.
   * @return boolean true if started otherwise false.
   */
  boolean isTrackerStarted();

}
