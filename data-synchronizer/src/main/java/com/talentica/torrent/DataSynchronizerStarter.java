package com.talentica.torrent;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.coordination.FileDownloaderListener;
import com.talentica.torrent.coordination.FileSeederListener;

/**
 * This is a starting point of data synchronizer application. It synchronizes any files' content
 * within cluster using torrent protocol.
 * 
 * @author nitink
 *
 */
public class DataSynchronizerStarter {

  public static final String TORRENT_PEERS_NODE_PATH = "/torrent/peers";

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSynchronizerStarter.class);

  public static void main(String[] args) {
    CuratorFramework client;
    try {
      validateProgramArguments(args);
      String connectionString = args[0];
      String host = args[1];
      RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
      client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
      client.start();
      FileSeederListener.register(client, host);
      FileDownloaderListener.register(client, host);
      String peerPath = TORRENT_PEERS_NODE_PATH + "/" + host;
      if (client.checkExists().forPath(peerPath) == null) {
        client.create().creatingParentsIfNeeded().forPath(peerPath);
      }
      synchronized (client) {
        client.wait();
      }
      LOGGER.info("Data synchronizer started successfully on:" + host);
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  private static void validateProgramArguments(String[] args) {
    if (args.length < 2) {
      System.err.println(
          "Please provide with arguments of zookepper connection string and host name/ip.");
      System.exit(1);
    }
  }

}
