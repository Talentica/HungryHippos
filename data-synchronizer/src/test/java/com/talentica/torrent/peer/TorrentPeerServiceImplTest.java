package com.talentica.torrent.peer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.torrent.tracker.TorrentTrackerServiceImpl;
import com.talentica.torrent.util.TorrentGenerator;
import com.turn.ttorrent.common.Torrent;

public class TorrentPeerServiceImplTest {

  private TorrentPeerServiceImpl torrentPeerServiceImpl;

  private File seedFilesDirectory;

  private File torrentFile;

  private int trackerPort = 6969;

  @Before
  public void setup() throws URISyntaxException, FileNotFoundException, IOException {
    TorrentTrackerServiceImpl.getInstance().startTracker("localhost", trackerPort);
    torrentFile = generateSampleTorrentFile();
    torrentPeerServiceImpl = new TorrentPeerServiceImpl();
  }

  private File generateSampleTorrentFile()
      throws URISyntaxException, FileNotFoundException, IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    File sourceFile =
        new File(classLoader.getResource("TestTorrentGenerationSourceFile.txt").getFile());
    List<URI> trackers = new ArrayList<>();
    trackers.add(new URI("http://localhost:" + trackerPort + "/announce"));
    Torrent torrent = TorrentGenerator.generate(sourceFile, trackers, "SampleSourceFile");
    seedFilesDirectory = new File(FilenameUtils.getFullPath(sourceFile.getAbsolutePath()));
    String torrentFilesDirectory = seedFilesDirectory.getAbsolutePath() + File.separator + ".."
        + File.separator + "testTorrentFiles";
    new File(torrentFilesDirectory).mkdirs();
    File torrentFile = new File(torrentFilesDirectory + File.separator + "test.torrent");
    FileOutputStream torrentFileOutputStream = new FileOutputStream(torrentFile);
    torrent.save(torrentFileOutputStream);
    torrentFileOutputStream.flush();
    torrentFileOutputStream.close();
    return torrentFile;
  }

  @Test
  public void testSeedFile() throws IOException {
    seedFile();
  }

  private void seedFile() throws IOException {
    TorrentTrackerServiceImpl.getInstance().newTorrentFileAvailable(torrentFile);
    torrentPeerServiceImpl.seedFile(FileUtils.readFileToByteArray(torrentFile), seedFilesDirectory,
        "localhost");
  }

  // @Test
  public void testDownloadFile() throws InterruptedException, IOException {
    seedFile();
    File downloadDir = new File(seedFilesDirectory.getAbsolutePath() + File.separator + ".."
        + File.separator + "downloadedFiles");
    try {
      downloadDir.mkdirs();
      torrentPeerServiceImpl.downloadFile(FileUtils.readFileToByteArray(torrentFile), downloadDir)
          .get();
      Optional<File> file = Arrays.asList(downloadDir.listFiles()).stream()
          .filter(name -> name.getName().equals("TestTorrentGenerationSourceFile.txt")).findFirst();
      assertNotNull(file.get());
      String content = FileUtils.readFileToString(file.get(), "UTF-8");
      assertNotNull(content);
      assertEquals("hello world..Torrent rocks!", content);
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  @After
  public void teardown() throws URISyntaxException, FileNotFoundException, IOException {
    TorrentTrackerServiceImpl.getInstance().stopTracker();
    torrentPeerServiceImpl = null;
  }

}
