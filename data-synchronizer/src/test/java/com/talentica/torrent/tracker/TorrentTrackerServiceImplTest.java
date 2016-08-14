package com.talentica.torrent.tracker;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TorrentTrackerServiceImplTest {

  private TorrentTrackerService torrentTrackerService;

  private int port = 7979;

  @Before
  public void setup() {
    torrentTrackerService = new TorrentTrackerServiceImpl();
  }

  @Test
  public void testStartTracker() {
    torrentTrackerService.startTracker("localhost", port);
    assertTrue(torrentTrackerService.isTrackerStarted());
  }

  @Test(expected = RuntimeException.class)
  public void testStartTrackerForAlreadyStartedTracker() {
    torrentTrackerService.startTracker("localhost", port + 1);
    torrentTrackerService.startTracker("localhost", port + 1);
  }

  @After
  public void teardown() {
    torrentTrackerService = null;
  }

}
