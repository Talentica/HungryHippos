package com.talentica.torrent;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.tracker.TorrentTrackerServiceImpl;

public class TorrentTrackerStarter {

  private static Logger LOGGER = LoggerFactory.getLogger(TorrentTrackerStarter.class);

  public static final String TRACKERS_NODE_PATH = "/torrent/trackers";

  public static void main(String[] args) {
    try {
      validateProgramArguments(args);
      String trackerHost = args[1];
      LOGGER.info("Starting tracker on:" + trackerHost);
      RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
      CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retryPolicy);
      client.start();
      String trackerPort = args[2];
      TorrentTrackerServiceImpl.getInstance().startTracker(trackerHost,
          Integer.parseInt(trackerPort));
      String trackerUrl = "http://" + trackerHost + ":" + trackerPort + "/announce";
      String trackerNodePath = TRACKERS_NODE_PATH + "/" + trackerHost;
      boolean pathExists = client.checkExists().forPath(trackerNodePath) != null;
      if (!pathExists) {
        client.create().creatingParentsIfNeeded().forPath(trackerNodePath, trackerUrl.getBytes());
      } else {
        client.setData().forPath(trackerNodePath, trackerUrl.getBytes());
      }
      LOGGER.info("Tracker started successfully at " + trackerHost + ":" + trackerPort);
      synchronized (client) {
        client.wait();
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
