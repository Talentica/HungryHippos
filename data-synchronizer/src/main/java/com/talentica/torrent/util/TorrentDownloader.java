package com.talentica.torrent.util;

import java.io.File;
import java.io.FileOutputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.map.ObjectMapper;

import com.talentica.torrent.FileMetadata;

/**
 * Utility that allows you to download torrent files from zookeeper server.
 * 
 * @author nitink
 *
 */
public class TorrentDownloader {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    validateArguments(args);
    String connectionString = args[0];
    String nodePath = args[1];
    String outputFilePath = args[2];
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(
        Environment.getCoordinationServerConnectionRetryBaseSleepTimeInMs(),
        Environment.getCoordinationServerConnectionRetryMaxTimes());
    CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    client.start();
    if (client.checkExists().forPath(nodePath) == null) {
      System.err.print("Node does not exist:" + nodePath);
      System.exit(1);
    }
    byte[] fileMetadataBytes = client.getData().forPath(nodePath);
    if (fileMetadataBytes == null || fileMetadataBytes.length == 0) {
      System.err.print("Data does not exist at:" + nodePath);
      System.exit(2);
    }
    FileMetadata fileMetadata = OBJECT_MAPPER.readValue(fileMetadataBytes, FileMetadata.class);
    File outputFile = new File(outputFilePath);
    outputFile.getParentFile().mkdirs();
    FileOutputStream output = new FileOutputStream(outputFile);
    output.write(DatatypeConverter.parseBase64Binary(fileMetadata.getBase64EncodedTorrentFile()));
    output.flush();
    output.close();
    client.close();
  }

  private static void validateArguments(String[] args) {
    if (args.length < 3) {
      System.out.println(
          "Please provide with parameters of zookeper connection string(comma seperated list of host:port), the zookeeper node to download torrent file from and file path to save torrent file at.");
      System.exit(1);
    }
  }

}
