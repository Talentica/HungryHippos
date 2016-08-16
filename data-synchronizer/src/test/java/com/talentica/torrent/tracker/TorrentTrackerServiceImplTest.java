package com.talentica.torrent.tracker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TorrentTrackerServiceImplTest {

  private int port = 7979;

  @Test
  public void testStartTracker() throws InterruptedException {
    TorrentTrackerServiceImpl.getInstance().stopTracker();
    assertFalse(TorrentTrackerServiceImpl.getInstance().isTrackerStarted());
    TorrentTrackerServiceImpl.getInstance().startTracker("localhost", port);
    assertTrue(TorrentTrackerServiceImpl.getInstance().isTrackerStarted());
    TorrentTrackerServiceImpl.getInstance().stopTracker();
  }

  @Test(expected = RuntimeException.class)
  public void testStartTrackerForAlreadyStartedTracker() throws InterruptedException {
    TorrentTrackerServiceImpl.getInstance().stopTracker();
    TorrentTrackerServiceImpl.getInstance().startTracker("localhost", port + 1);
    TorrentTrackerServiceImpl.getInstance().startTracker("localhost", port + 1);
  }

}
