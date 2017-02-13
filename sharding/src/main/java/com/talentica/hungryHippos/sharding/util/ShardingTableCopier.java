package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.torrent.util.Environment;

/**
 * Utility class to copy generated sharding tables on nodes/local file system.
 * 
 * @author nitink
 *
 */
public class ShardingTableCopier {

  public static final String FILES_DOWNLOAD_SUCCESS_NODE_PATH =
      Environment.getPropertyValue("files.download.success.node.path");

  public static final String SHARDING_ZIP_FILE_NAME = "sharding-table";

  private ShardingClientConfig shardingClientConfig;
  private String sourceDirectoryContainingShardingFiles;

  public ShardingTableCopier(String sourceDirectoryContainingShardingFiles,
      ShardingClientConfig shardingClientConfig) {
    this.shardingClientConfig = shardingClientConfig;
    this.sourceDirectoryContainingShardingFiles = sourceDirectoryContainingShardingFiles;
  }


  public static final String FILES_ERRED_WHILE_DOWNLOAD_NODE_PATH =
      Environment.getPropertyValue("files.erred.while.download.node.path");

  /**
   * Copies sharding table files from
   * 
   * @param randomNode
   */
  public void copyToAllNodeInCluster(List<Node> nodes) {
    try {
      List<String> processIgnores = new ArrayList<>(0);
      String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
      String distributedFilePath = shardingClientConfig.getInput().getDistributedFilePath();
      String zipFilePath = TarAndGzip.folder(new File(sourceDirectoryContainingShardingFiles),
          processIgnores, SHARDING_ZIP_FILE_NAME);
      String destinationDirectory =
          fileSystemBaseDirectory + distributedFilePath + File.separatorChar  + SHARDING_ZIP_FILE_NAME + ".tar.gz";


      int optimumNumberOfThreads = Runtime.getRuntime().availableProcessors();
      ExecutorService executorService = Executors.newFixedThreadPool(optimumNumberOfThreads);
      ShardingFileUploader[] shardingFileUploader = new ShardingFileUploader[nodes.size()];

      for (int i = 0; i < nodes.size(); i++) {
        shardingFileUploader[i] =
            new ShardingFileUploader(nodes.get(i), zipFilePath, destinationDirectory);
        executorService.execute(shardingFileUploader[i]);
      }

      executorService.shutdown();
      while (!executorService.isTerminated()) {

      }
      boolean success = true;
 
      for (int i = 0; i < shardingFileUploader.length; i++) {
        success = success && shardingFileUploader[i].isSuccess();
        String host = shardingFileUploader[i].getNode().getIp();
        if (success) {
          String fileDownloadSuccessNodePath =
              FILES_DOWNLOAD_SUCCESS_NODE_PATH + destinationDirectory + "/" + host;

          updateCoordinationServer(fileDownloadSuccessNodePath);

        } else {
          String fileDownloadFailedNodePath =
              FILES_ERRED_WHILE_DOWNLOAD_NODE_PATH + destinationDirectory + "/" + host;
          updateCoordinationServer(fileDownloadFailedNodePath);
        }
      }
      if (!success) {
        throw new RuntimeException("File Publish failed");
      }

    } catch (JAXBException | IOException | HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // Sync the details;

  }

  private void updateCoordinationServer(String path)
      throws FileNotFoundException, JAXBException, IOException, HungryHippoException {

    HungryHippoCurator curator = HungryHippoCurator.getInstance();
    try {
      curator.createPersistentNode(path);
    } catch (HungryHippoException exception) {
      throw new RuntimeException(exception);
    }
  }

}


;
