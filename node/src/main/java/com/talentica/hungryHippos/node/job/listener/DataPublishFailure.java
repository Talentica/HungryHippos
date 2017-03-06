package com.talentica.hungryHippos.node.job.listener;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.util.Map;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;


/**
 * 
 * {@code NewTorrentAvailableListener} updates whenever a new torrent file is available to the
 * system.
 */
public class DataPublishFailure {

  private static final Logger logger = LoggerFactory.getLogger(DataPublishFailure.class);
  private static NodeCache nodeCache;
  private static NodeCacheListener nodeCacheListener;
  private static HungryHippoCurator client = HungryHippoCurator.getInstance();
  private static String destinationPathNode = null;
  // private static final String ERROR_MESSAGE = "client is disconnected";



  private DataPublishFailure() {}

  public static void register(String path) {

    destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + path;
    String dataPublishStarted = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
        + FileSystemConstants.CLIENT_UPLOADS_IN_PARALLEL + HungryHippoCurator.ZK_PATH_SEPERATOR
        + FileSystemConstants.DATA_PUBLISH_STARTED;

    logger.info("datapublish started {}", dataPublishStarted);
    nodeCache = new NodeCache(client.getCurator(), dataPublishStarted);
    try {

      logger.info("added listener to the node {}", dataPublishStarted);

      nodeCache.getListenable().addListener(nodeCacheListener = new NodeCacheListener() {

        @Override
        public void nodeChanged() throws Exception {
          ChildData data = nodeCache.getCurrentData();
          if (data == null) {

            try {

              logger.info("Ephemeral node is removed " + dataPublishStarted);
              String dataReady = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
                  + FileSystemConstants.DATA_READY;
              String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
                  + FileSystemConstants.PUBLISH_FAILED;
              /*
               * String failureLogs = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR +
               * FileSystemConstants.CLIENT_UPLOADS_IN_PARALLEL +
               * HungryHippoCurator.ZK_PATH_SEPERATOR + FileSystemConstants.FAILURE_LOGS;
               */
              logger.info("dataReady Node is {}", dataReady);

              // sleep for some time
              Thread.sleep(2000);
              if (client.checkExists(dataReady)) {
                // Datapublish was successful;
                logger.info("dataPublish was successful");
              } else {
                client.createPersistentNodeIfNotPresent(pathForFailureNode);
                /*
                 * client.createPersistentNodeIfNotPresent( failureLogs +
                 * HungryHippoCurator.ZK_PATH_SEPERATOR + NodeInfo.INSTANCE.getId(), ERROR_MESSAGE);
                 */
                Thread.sleep(3000);
                truncate(path);
              }
              // new FileTruncateService(inputDir).truncate();

            } catch (Exception exception) {
              exception.printStackTrace();

              logger.error("Truncate failed");
            } finally {
              logger.info("finished removing listener");
              removeListener();
            }
          } else {

            logger.info("node not deleted");
          }
        }
      });
      logger.info("node cache is started");
      nodeCache.start();
      /*
       * synchronized (client) { try { client.wait(); } catch (InterruptedException e) { // TODO
       * Auto-generated catch block e.printStackTrace(); }
       * 
       * }
       */
    } catch (Exception e) {
      e.printStackTrace();

    }

  }



  private static void removeListener() {

    nodeCache.getListenable().removeListener(nodeCacheListener);

    try {
      nodeCache.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }



  private static void truncate(String baseFolderPath)
      throws FileNotFoundException, IOException, HungryHippoException {

    String metadataFilePath = FileSystemContext.getRootDirectory() + File.separator + baseFolderPath
        + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME + File.separator
        + NodeInfo.INSTANCE.getId();
    logger.info("current nodes file details are present in this file {}", metadataFilePath);
    String dataFolderPath = FileSystemContext.getRootDirectory() + File.separator + baseFolderPath
        + File.separator + FileSystemContext.getDataFilePrefix();
    File dataFolder = new File(dataFolderPath);
    logger.info("dataFolder Path is {}", dataFolderPath);
    File metadataFolder = new File(metadataFilePath);
    String restoration = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
        + FileSystemConstants.CLIENT_UPLOADS_IN_PARALLEL + HungryHippoCurator.ZK_PATH_SEPERATOR
        + FileSystemConstants.RESTORATION_DETAILS;
    int nodeId = NodeInfo.INSTANCE.getIdentifier();
    if (dataFolder.isDirectory()) {

      int numberOfFiles = dataFolder.listFiles().length;

      logger.info("restoring {} files to the previous stable state in nodeId {}", numberOfFiles,
          nodeId);
      client.createPersistentNode(restoration + HungryHippoCurator.ZK_PATH_SEPERATOR + nodeId,
          String.valueOf(numberOfFiles));

    }
    if (metadataFolder.exists()) {
      try (ObjectInputStream objectInputStream =
          new ObjectInputStream(new FileInputStream(metadataFolder))) {
        Map<String, Long> fileNametoSizeMap = (Map<String, Long>) objectInputStream.readObject();
        for (Map.Entry<String, Long> entrySet : fileNametoSizeMap.entrySet()) {
          String fileName = entrySet.getKey();
          Long length = entrySet.getValue();
          String absolutePath = dataFolderPath + File.separatorChar + fileName;
          RandomAccessFile raf = new RandomAccessFile(absolutePath, "rwd");
          raf.setLength(length);
          raf.close();
        }

        logger.info("Reverted the changes back to the stable state");


      } catch (ClassNotFoundException e) {
        logger.error("not able to load the class reason for this is {}", e.getMessage());
      }

    } else {

      logger.info("deleting all the files as the data publish for the first time is failed");
      // delete all the file if datapublisher failed for first time
      File[] listOfFiles = dataFolder.listFiles();
      for (File file : listOfFiles) {
        file.delete();
      }
      logger.info("deleted {} files", listOfFiles.length);
    }


    if (client.checkExists(restoration)) {
      client
          .deletePersistentNodeIfExits(restoration + HungryHippoCurator.ZK_PATH_SEPERATOR + nodeId);

    }

  }
}
