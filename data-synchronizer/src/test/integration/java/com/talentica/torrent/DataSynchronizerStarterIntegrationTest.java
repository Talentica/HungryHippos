package com.talentica.torrent;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.torrent.coordination.NewTorrentAvailableListener;
import com.talentica.torrent.util.Environment;
import com.talentica.torrent.util.TorrentGenerator;

public class DataSynchronizerStarterIntegrationTest {

  private static final String ZOOKEEPER_CONN_STRING = "localhost:2181";

  private static final String ORIGIN_HOST = "localhost";

  private static final String TRACKER_HOST = "localhost";

  private static final String TRACKER_PORT = "6969";

  private String SEED_FILE_PATH = "";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Thread START_TRACKER_THREAD = new Thread(new Runnable() {
    @Override
    public void run() {
      TorrentTrackerStarter.main(new String[] {ZOOKEEPER_CONN_STRING, TRACKER_HOST, TRACKER_PORT});

    }
  });

  private static final Thread START_DATA_SYNCHRONIZER_THREAD = new Thread(new Runnable() {
    @Override
    public void run() {

      DataSynchronizerStarter.main(new String[] {ZOOKEEPER_CONN_STRING, ORIGIN_HOST});
    }
  });



  private CuratorFramework client;

  @Before
  public void setup() throws InterruptedException {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    SEED_FILE_PATH = classLoader.getResource("TestTorrentGenerationSourceFile.txt").getPath();
    START_TRACKER_THREAD.start();
    Thread.sleep(10000);
    START_DATA_SYNCHRONIZER_THREAD.start();
    Thread.sleep(10000);
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(
        Environment.getCoordinationServerConnectionRetryBaseSleepTimeInMs(),
        Environment.getCoordinationServerConnectionRetryMaxTimes());
    client = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONN_STRING, retryPolicy);
    client.start();
  }

  @Test
  public void testSeedAndDownloadOfSampleFile()
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    FileMetadata fileMetadata = new FileMetadata();
    fileMetadata.setOriginHost(ORIGIN_HOST);
    fileMetadata.setPath(SEED_FILE_PATH);
    File torrentFile = TorrentGenerator.generateTorrentFile(client, new File(SEED_FILE_PATH));
    fileMetadata.setBase64EncodedTorrentFile(
        DatatypeConverter.printBase64Binary(FileUtils.readFileToByteArray(torrentFile)));
    client.create().creatingParentsIfNeeded().forPath(
        NewTorrentAvailableListener.NEW_TORRENT_AVAILABLE_NODE_PATH + "/"
            + System.currentTimeMillis(),
        OBJECT_MAPPER.writeValueAsString(fileMetadata).getBytes());
    synchronized (fileMetadata) {
      fileMetadata.wait();
    }
  }

  @After
  public void teardown() {

    START_DATA_SYNCHRONIZER_THREAD.interrupt();
    START_TRACKER_THREAD.interrupt();
    client.close();
  }

}
