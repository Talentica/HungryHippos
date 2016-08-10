package com.talentica.torrent.tracker;

import java.io.File;
import java.util.Collection;

import com.turn.ttorrent.tracker.TrackedTorrent;

public interface TorrentTrackerService {

  /**
   * Starts a new tracker on localhost at the specified port. Throws error if tracker was already
   * started on specified port earlier.
   * 
   * @param port
   */
  public void startTracker(int port);

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

  /**
   * Returns unmodifiable collection of torrents available.
   * 
   * @param port
   * @return
   */
  public Collection<TrackedTorrent> getAvailableTorrents();

  boolean isTrackerStarted();

}
