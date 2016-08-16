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

  private static final String FILE_TO_SEED = "/root/test";

  private static final String ZOOKEEPER_CONN_STRING = "138.68.17.228:2181";

  private static final String ORIGIN_HOST = "138.68.17.228";

  private static final String TRACKER_HOST = "138.68.17.228";

  private static final String TRACKER_PORT = "6969";

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

  public void setup() {
    START_TRACKER_THREAD.start();
    START_DATA_SYNCHRONIZER_THREAD.start();
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
    client = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONN_STRING, retryPolicy);
    client.start();
  }

  public void seedFile()
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    FileMetadata fileMetadata = new FileMetadata();
    fileMetadata.setOriginHost(ORIGIN_HOST);
    fileMetadata.setPath(FILE_TO_SEED);
    client.create().creatingParentsIfNeeded().forPath(
        FileSeederListener.SEED_FILES_PARENT_NODE_PATH + ORIGIN_HOST + "/"
            + System.currentTimeMillis(),
        OBJECT_MAPPER.writeValueAsString(fileMetadata).getBytes());
    synchronized (fileMetadata) {
      fileMetadata.wait();
    }
  }

  public void teardown() {
    START_DATA_SYNCHRONIZER_THREAD.interrupt();
    START_TRACKER_THREAD.interrupt();
    client.close();
  }

  public static void main(String[] args)
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    TestSeedFile testseedfile = new TestSeedFile();
    try {
      testseedfile.setup();
      testseedfile.seedFile();
    } finally {
      testseedfile.teardown();
    }
  }


}
