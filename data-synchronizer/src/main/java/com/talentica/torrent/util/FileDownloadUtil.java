package com.talentica.torrent.util;

import java.io.IOException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.talentica.torrent.coordination.FileDownloaderListener;

public class FileDownloadUtil {

  public static void main(String[] args)
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    if (args.length < 2) {
      System.err.println(
          "Please provide the IP of host to download file on and zookepper connection string.");
      System.exit(-1);
    }
    startDownload(args);
  }

  private static void startDownload(String[] args) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        CuratorFramework client;
        try {
          RetryPolicy retryPolicy = new ExponentialBackoffRetry(
              Environment.getCoordinationServerConnectionRetryBaseSleepTimeInMs(),
              Environment.getCoordinationServerConnectionRetryMaxTimes());
          client = CuratorFrameworkFactory.newClient(args[1], retryPolicy);
          client.start();
          FileDownloaderListener.register(client, args[0]);
          synchronized (client) {
            client.wait();
          }
        } catch (Exception exception) {
          throw new RuntimeException(exception);
        }
      }
    }).start();
  }

}
