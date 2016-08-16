package com.talentica.torrent.tracker;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TorrentTrackerServiceImplTest {

  private int port = 7979;

  @Test
  public void testStartTracker() {
    TorrentTrackerServiceImpl.getInstance().startTracker("localhost", port);
    assertTrue(TorrentTrackerServiceImpl.getInstance().isTrackerStarted());
  }

  @Test(expected = RuntimeException.class)
  public void testStartTrackerForAlreadyStartedTracker() {
    TorrentTrackerServiceImpl.getInstance().startTracker("localhost", port + 1);
    TorrentTrackerServiceImpl.getInstance().startTracker("localhost", port + 1);
  }

}
