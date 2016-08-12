package com.talentica.torrent;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.tracker.TorrentTrackerService;
import com.talentica.torrent.tracker.TorrentTrackerServiceImpl;

public class TorrentTrackerStarter {

  private static Logger LOGGER = LoggerFactory.getLogger(TorrentTrackerStarter.class);

  public static final String TRACKERS_NODE_PATH = "/torrent/trackers";

  private static final TorrentTrackerService TORRENT_TRACKER_SERVICE =
      new TorrentTrackerServiceImpl();

  public static void main(String[] args) {
    try {
      validateProgramArguments(args);
      RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
      CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retryPolicy);
      client.start();
      String trackerHost = args[1];
      TORRENT_TRACKER_SERVICE.startTracker(trackerHost, Integer.parseInt(args[2]));
      String trackerUrl = "http://" + trackerHost + ":" + args[2] + "/announce";
      String trackerNodePath = TRACKERS_NODE_PATH + "/" + trackerHost;
      boolean pathExists = client.checkExists().forPath(trackerNodePath) != null;
      if (!pathExists) {
        client.create().creatingParentsIfNeeded().forPath(trackerNodePath, trackerUrl.getBytes());
      } else {
        client.setData().forPath(trackerNodePath, trackerUrl.getBytes());
      }
    } catch (Exception exception) {
      LOGGER.error("Error occurred while starting up tracker.", exception);
    }
  }

  private static void validateProgramArguments(String[] args) {
    if (args.length < 3) {
      System.err.println(
          "Please provide with arguments of zookepper connection string,host name and port to start tracker on.");
      System.exit(1);
    }
  }

}
