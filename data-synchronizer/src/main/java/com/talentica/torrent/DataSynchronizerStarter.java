package com.talentica.torrent;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.coordination.FileDownloaderListener;
import com.talentica.torrent.coordination.FileSeederListener;

public class DataSynchronizerStarter {

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
      synchronized (client) {
        client.wait();
      }
      LOGGER.info("Data synchronizer started successfully.");
    } catch (InterruptedException exception) {
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
