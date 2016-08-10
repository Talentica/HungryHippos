package com.talentica.torrent.tracker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.turn.ttorrent.tracker.TrackedTorrent;

public class TorrentTrackerServiceImplTest {

  private static TorrentTrackerService torrentTrackerService;

  private int port = 7979;

  @BeforeClass
  public static void setup() {
    torrentTrackerService = new TorrentTrackerServiceImpl();
  }

  @Test
  public void testStartTracker() {
    torrentTrackerService.startTracker(port);
    assertTrue(torrentTrackerService.isTrackerStarted());
  }

  @Test
  public void testNewTorrentFileAvailable() {
    File torrentFile =
        new File(getClass().getClassLoader().getResource("sample.torrent").getFile());
    torrentTrackerService.newTorrentFileAvailable(torrentFile);
    Collection<TrackedTorrent> availableTorrents = torrentTrackerService.getAvailableTorrents();
    assertNotNull(availableTorrents);
    assertFalse(availableTorrents.isEmpty());
    List<TrackedTorrent> matchingtorrents = availableTorrents.stream()
        .filter(availableTorrent -> availableTorrent.getFilenames().contains("sample.txt"))
        .collect(Collectors.toList());
    assertNotNull(matchingtorrents);
    assertFalse(matchingtorrents.isEmpty());
  }

  @Test
  public void testStopTracker() {
    torrentTrackerService.stopTracker();
    assertFalse(torrentTrackerService.isTrackerStarted());
  }


  @AfterClass
  public static void teardown() {
    torrentTrackerService = null;
  }



}
