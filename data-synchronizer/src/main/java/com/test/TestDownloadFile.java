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

  private static final String ZOOKEEPER_CONN_STRING = "localhost";

  private static final String DOWNLOAD_HOST = "localhost";

  private static final Thread START_DOWNLOADER_THREAD = new Thread(new Runnable() {
    @Override
    public void run() {
      CuratorFramework client;
      try {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 15);
        client = CuratorFrameworkFactory.newClient(ZOOKEEPER_CONN_STRING, retryPolicy);
        client.start();
        FileDownloaderListener.register(client, DOWNLOAD_HOST);
        synchronized (client) {
          client.wait();
        }
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }
    }
  });

  public static void main(String[] args)
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    START_DOWNLOADER_THREAD.start();
  }

}
