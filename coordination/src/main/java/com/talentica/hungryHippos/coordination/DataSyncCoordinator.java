package com.talentica.hungryHippos.coordination;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.torrent.coordination.FileDownloaderListener;
import com.talentica.torrent.coordination.FileSynchronizerListener;

/**
 * This class is for coordinating with the Data-synchronizer by notifying and getting information.
 * @author rajkishoreh 
 * @since 22/8/16.
 */
public class DataSyncCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSyncCoordinator.class);



  /**
   * Notifies Data-synchronizer for file sync up
   *
   * @param hostIP
   * @param uploadDestinationPath
   * @throws Exception
   */
  public static void notifyFileSync(String hostIP, String uploadDestinationPath) throws Exception {
    LOGGER.info("Notifying File sync up hotsIP :{} file");
    LOGGER.info("hotsIP :{} file :{}", hostIP, uploadDestinationPath);
    HungryHippoCurator curator = HungryHippoCurator.getInstance();
    curator.createPersistentNode(
        FileSynchronizerListener.FILES_FOR_SYNC + hostIP + "/" + +System.currentTimeMillis(),
        uploadDestinationPath);


  }

  /**
   * Checks the sync up status of the file
   *
   * @param seedFilePath
   * @return
   * @throws Exception
   */
  public static boolean checkSyncUpStatus(String seedFilePath) throws Exception {
    LOGGER.info("Checking Sync Up Status of :{}", seedFilePath);
    HungryHippoCurator curator = HungryHippoCurator.getInstance();

    List<Node> nodeList = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
    int totalNoOfNodes = nodeList.size();
    int requiredSuccessNodes = totalNoOfNodes - 1;
    if (requiredSuccessNodes == 0)
      return true;
    String nodeForCheckingSuccess =
        FileDownloaderListener.FILES_DOWNLOAD_SUCCESS_NODE_PATH + seedFilePath;
    String nodeForCheckingFailure = FileDownloaderListener.FILES_ERRED_WHILE_DOWNLOAD_NODE_PATH
        + StringUtils.substringAfterLast(seedFilePath, "/");
    while (true) {
      Stat successNodeStat = curator.getStat(nodeForCheckingSuccess);
      if (successNodeStat != null) {
        int numSuccessNodes = successNodeStat.getNumChildren();
        if (numSuccessNodes == requiredSuccessNodes) {

          return true;
        }
      }

      Stat failureNodeStat = curator.getStat(nodeForCheckingFailure);
      if (failureNodeStat != null) {
        int numFailureNodes = failureNodeStat.getNumChildren();
        if (numFailureNodes > 0) {
          return false;
        }
      }
    }

  }
}
