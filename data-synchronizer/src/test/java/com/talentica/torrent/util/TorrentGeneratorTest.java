package com.talentica.torrent.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;
import com.turn.ttorrent.common.Torrent;


public class TorrentGeneratorTest {

  @Test
  public void testGenerate() throws URISyntaxException, IOException {
    File sourceFile = new File(
        getClass().getClassLoader().getResource("TestTorrentGenerationSourceFile.txt").getFile());
    List<URI> trackers = new ArrayList<>();
    trackers.add(new URI("http://localhost:6969/announce"));
    Torrent torrent = TorrentGenerator.generate(sourceFile, trackers, "TestSystem");
    assertNotNull(torrent);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    torrent.save(byteArrayOutputStream);
    byte[] bytes = byteArrayOutputStream.toByteArray();
    assertNotNull(bytes);
    assertTrue(bytes.length > 0);
    assertTrue(bytes[0] != 0);
  }

}
