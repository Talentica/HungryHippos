package com.talentica.torrent.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import com.turn.ttorrent.common.Torrent;


public class TorrentGeneratorTest {

  @Test
  public void testGenerate() throws URISyntaxException, IOException {
    File sourceFile = new File(
        getClass().getClassLoader().getResource("TestTorrentGenerationSourceFile.txt").getFile());
    Torrent torrent =
        TorrentGenerator.generate(sourceFile, new URI("localhost:6969"), "TestSystem");
    assertNotNull(torrent);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    torrent.save(byteArrayOutputStream);
    byte[] bytes = byteArrayOutputStream.toByteArray();
    assertNotNull(bytes);
    assertTrue(bytes.length > 0);
    assertTrue(bytes[0] != 0);
  }

}
