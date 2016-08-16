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
import com.talentica.torrent.util.TorrentGenerator;

public class DataSynchronizerStarterIntegrationTest {

  private static final String ZOOKEEPER_CONN_STRING = "138.68.17.228:2181";

  private static final String ORIGIN_HOST = "138.68.17.228";

  private static final String TRACKER_HOST = "138.68.17.228";

  private static final String TRACKER_PORT = "6969";

  private static final String SEED_FILE_PATH = "/root/testNewFile1";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Thread START_TRACKER_DATA_SYNCHRONIZER_THREAD = new Thread(new Runnable() {
    @Override
    public void run() {
      TorrentTrackerStarter.main(new String[] {ZOOKEEPER_CONN_STRING, TRACKER_HOST, TRACKER_PORT});
      DataSynchronizerStarter.main(new String[] {ZOOKEEPER_CONN_STRING, ORIGIN_HOST});
    }
  });

  private CuratorFramework client;

  @Before
  public void setup() {
    START_TRACKER_DATA_SYNCHRONIZER_THREAD.start();
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
    client = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONN_STRING, retryPolicy);
    client.start();
  }

  @Test
  public void testSeedAndDownloadOfSampleFile()
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    FileMetadata fileMetadata = new FileMetadata();
    fileMetadata.setOriginHost(ORIGIN_HOST);
    fileMetadata.setPath(SEED_FILE_PATH);
    File torrentFile = TorrentGenerator.generateTorrentFile(client,
        new File("/home/nitink/hhfs/node1file/testNewFile1"));
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
    START_TRACKER_DATA_SYNCHRONIZER_THREAD.interrupt();
    client.close();
  }

}
