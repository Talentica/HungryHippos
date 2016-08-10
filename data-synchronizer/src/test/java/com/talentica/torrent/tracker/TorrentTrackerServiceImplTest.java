package com.talentica.torrent.tracker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

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
    torrentTrackerService.startTracker(port);
    assertTrue(torrentTrackerService.isTrackerStarted());
  }

  @Test(expected = RuntimeException.class)
  public void testStartTrackerForAlreadyStartedTracker() {
    torrentTrackerService.startTracker(port + 1);
    torrentTrackerService.startTracker(port + 1);
  }

  @Test(expected = RuntimeException.class)
  public void testNewTorrentFileAvailableOnNotStartedTracker() {
    File torrentFile =
        new File(getClass().getClassLoader().getResource("sample.torrent").getFile());
    torrentTrackerService.newTorrentFileAvailable(torrentFile);
  }

  @Test
  public void testNewTorrentFileAvailable() {
    torrentTrackerService.startTracker(port);
    assertFalse(torrentTrackerService.isTorrentAvailableForFileName("sample.txt"));
    File torrentFile =
        new File(getClass().getClassLoader().getResource("sample.torrent").getFile());
    torrentTrackerService.newTorrentFileAvailable(torrentFile);
    assertTrue(torrentTrackerService.isTorrentAvailableForFileName("sample.txt"));
  }

  @Test
  public void testStopTracker() {
    torrentTrackerService.startTracker(port);
    assertTrue(torrentTrackerService.isTrackerStarted());
    torrentTrackerService.stopTracker();
    assertFalse(torrentTrackerService.isTrackerStarted());
  }

  @After
  public void teardown() {
    torrentTrackerService = null;
  }

}
