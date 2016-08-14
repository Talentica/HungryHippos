package com.test;

import java.io.IOException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.talentica.torrent.DataSynchronizerStarter;
import com.talentica.torrent.FileMetadata;
import com.talentica.torrent.TorrentTrackerStarter;
import com.talentica.torrent.coordination.FileSeederListener;

public class TestSeedFile {

  private static final String ZOOKEEPER_CONN_STRING = "138.68.49.42:2181";

  private static final String ORIGIN_HOST = "138.68.49.42";

  private static final String TRACKER_HOST = "138.68.49.42";

  private static final String TRACKER_PORT = "6969";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Thread START_TRACKER_DATA_SYNCHRONIZER_THREAD = new Thread(new Runnable() {
    @Override
    public void run() {
      TorrentTrackerStarter.main(new String[] {ZOOKEEPER_CONN_STRING, TRACKER_HOST, TRACKER_PORT});
      DataSynchronizerStarter.main(new String[] {ZOOKEEPER_CONN_STRING, ORIGIN_HOST});
    }
  });

  private CuratorFramework client;

  public void setup() {
    START_TRACKER_DATA_SYNCHRONIZER_THREAD.start();
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
    client = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONN_STRING, retryPolicy);
    client.start();
  }

  public void seedFile()
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    FileMetadata fileMetadata = new FileMetadata();
    fileMetadata.setOriginHost(ORIGIN_HOST);
    fileMetadata.setPath("/root/test");
    client.create().creatingParentsIfNeeded().forPath(
        FileSeederListener.SEED_FILES_PARENT_NODE_PATH + ORIGIN_HOST + "/"
            + System.currentTimeMillis(),
        OBJECT_MAPPER.writeValueAsString(fileMetadata).getBytes());
    synchronized (fileMetadata) {
      fileMetadata.wait();
    }
  }

  public void teardown() {
    START_TRACKER_DATA_SYNCHRONIZER_THREAD.interrupt();
    client.close();
  }

  public static void main(String[] args)
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    TestSeedFile testTorrent = new TestSeedFile();
    try {
      testTorrent.setup();
      testTorrent.seedFile();
    } finally {
      testTorrent.teardown();
    }
  }


}
