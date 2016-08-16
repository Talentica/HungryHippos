package com.test;

import java.io.IOException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.talentica.torrent.coordination.FileDownloaderListener;

public class TestDownloadFile {

  private static final String ZOOKEEPER_CONN_STRING = "138.68.17.228:2181";

  public static void main(String[] args)
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    if (args.length < 1) {
      System.err.println("Please provide the IP of host to download file on.");
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
          RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
          client = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONN_STRING, retryPolicy);
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
