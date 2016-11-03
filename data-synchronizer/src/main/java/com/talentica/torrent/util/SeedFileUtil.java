package com.talentica.torrent.util;

import java.io.IOException;

import org.apache.commons.lang3.BooleanUtils;
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
/**
 * {@code SeedFileUtil} seeds the file that comes in torrent.
 *
 *
 */
public class SeedFileUtil {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args)
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    Thread START_TRACKER_THREAD = null;
    Thread START_DATA_SYNCHRONIZER_THREAD = null;
    CuratorFramework client = null;
    try {
      validateArgs(args);
      String FILE_TO_SEED = args[0];
      String ZOOKEEPER_CONN_STRING = args[1];
      String ORIGIN_HOST = args[2];
      String TRACKER_HOST = args[3];
      String TRACKER_PORT = args[4];
      boolean startTracker = BooleanUtils.toBoolean(args[5]);

      RetryPolicy retryPolicy = new ExponentialBackoffRetry(
          Environment.getCoordinationServerConnectionRetryBaseSleepTimeInMs(),
          Environment.getCoordinationServerConnectionRetryMaxTimes());
      client = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONN_STRING, retryPolicy);
      client.start();

      START_TRACKER_THREAD = new Thread(new Runnable() {
        @Override
        public void run() {
          TorrentTrackerStarter
              .main(new String[] {ZOOKEEPER_CONN_STRING, TRACKER_HOST, TRACKER_PORT});
        }
      });
      if (startTracker) {
        START_TRACKER_THREAD.start();
      }

      START_DATA_SYNCHRONIZER_THREAD = new Thread(new Runnable() {
        @Override
        public void run() {
          DataSynchronizerStarter.main(new String[] {ZOOKEEPER_CONN_STRING, ORIGIN_HOST});
        }
      });
      START_DATA_SYNCHRONIZER_THREAD.start();
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
    } finally {
      cleanup(START_TRACKER_THREAD, START_DATA_SYNCHRONIZER_THREAD, client);
    }
  }

  private static void cleanup(Thread START_TRACKER_THREAD, Thread START_DATA_SYNCHRONIZER_THREAD,
      CuratorFramework client) {
    interruptThread(START_DATA_SYNCHRONIZER_THREAD);
    interruptThread(START_TRACKER_THREAD);
    if (client != null) {
      client.close();
    }
  }

  private static void interruptThread(Thread thread) {
    if (thread != null && thread.isAlive()) {
      thread.interrupt();
    }
  }

  private static void validateArgs(String[] args) {
    if (args.length < 6) {
      System.err.println(
          "please provide with following required parameters:path to file to seed,zookepper connection string,origin host, tracker host, tracker port, boolean to start tracker on this node or not.");
      System.exit(-1);
    }
  }

}
