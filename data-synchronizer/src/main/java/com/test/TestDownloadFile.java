package com.test;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.talentica.torrent.DataSynchronizerStarter;

public class TestDownloadFile {

  private static final String ZOOKEEPER_CONN_STRING = "138.68.49.42:2181";

  private static final String DOWNLOAD_HOST = "138.68.49.40";

  private static final Thread START_DOWNLOADER_THREAD = new Thread(new Runnable() {
    @Override
    public void run() {
      DataSynchronizerStarter.main(new String[] {ZOOKEEPER_CONN_STRING, DOWNLOAD_HOST});
    }
  });

  public static void main(String[] args)
      throws JsonGenerationException, JsonMappingException, IOException, Exception {
    START_DOWNLOADER_THREAD.start();
  }

}
