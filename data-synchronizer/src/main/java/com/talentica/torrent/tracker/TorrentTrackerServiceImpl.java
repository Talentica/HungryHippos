package com.talentica.torrent.tracker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.turn.ttorrent.tracker.TrackedTorrent;
import com.turn.ttorrent.tracker.Tracker;

public class TorrentTrackerServiceImpl implements TorrentTrackerService {

  private Tracker tracker;

  private int port = -1;

  @Override
  public void startTracker(int port) {
    try {
      if (tracker != null) {
        throw new RuntimeException("Tracker already started on port:" + port);
      }
      tracker = new Tracker(new InetSocketAddress(port));
      tracker.start();
      this.port = port;
    } catch (IOException exception) {
      stopTracker();
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void newTorrentFileAvailable(File torrentFile) {
    try {
      if (tracker == null) {
        throw new RuntimeException("Tracker not started on port:" + port);
      }
      TrackedTorrent trackedTorrent = TrackedTorrent.load(torrentFile);
      tracker.announce(trackedTorrent);
    } catch (IOException | NoSuchAlgorithmException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void stopTracker() {
    if (tracker != null) {
      tracker.stop();
      port = -1;
    }
  }

  @Override
  public boolean isTrackerStarted() {
    return port != -1;
  }

  @Override
  public boolean isTorrentAvailableForFileName(String filename) {
    Collection<TrackedTorrent> availableTorrents = tracker.getTrackedTorrents();
    List<TrackedTorrent> matchingtorrents = availableTorrents.stream()
        .filter(availableTorrent -> availableTorrent.getFilenames().contains(filename))
        .collect(Collectors.toList());
    return matchingtorrents != null && !matchingtorrents.isEmpty();
  }

}
