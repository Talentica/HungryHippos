package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.scp.Jscp;
import com.talentica.hungryHippos.utility.scp.SecureContext;
import com.talentica.hungryhippos.config.client.Output;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * Utility class to copy generated sharding tables on nodes/local file system.
 * 
 * @author nitink
 *
 */
public class ShardingTableCopier {

  public static final String SHARDING_ZIP_FILE_NAME = "sharding-table";

  private static final String SHARDING_TABLE_AVAILABLE_WITH_NODE_PATH =
      "/sharding-table-available-with/";

  private static final String SHARDING_TABLE_TO_BE_COPIED_ON_NODE_PATH =
      "/sharding-table-to-be-copied-on-node/";

  private ShardingClientConfig shardingClientConfig;
  private Output outputConfiguration;
  private String sourceDirectoryContainingShardingFiles;

  public ShardingTableCopier(String sourceDirectoryContainingShardingFiles,
      ShardingClientConfig shardingClientConfig, Output outputConfiguration) {
    this.shardingClientConfig = shardingClientConfig;
    this.outputConfiguration = outputConfiguration;
    this.sourceDirectoryContainingShardingFiles = sourceDirectoryContainingShardingFiles;
  }

  /**
   * Copies sharding table files from
   * 
   * @param randomNode
   */
  public void copyToRandomNodeInCluster(Node randomNode) {
    try {
      String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
      List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
      String nodeSshUsername = outputConfiguration.getNodeSshUsername();
      String nodeSshPrivateKeyFilePath = outputConfiguration.getNodeSshPrivateKeyFilePath();
      File privateKeyFile = new File(nodeSshPrivateKeyFilePath);
      String host = randomNode.getIp();
      String distributedFilePath = shardingClientConfig.getInput().getDistributedFilePath();
      String destinationDirectory = fileSystemBaseDirectory + distributedFilePath;
      SecureContext context = new SecureContext(nodeSshUsername, host);
      context.setPrivateKeyFile(privateKeyFile);
      Jscp.scpTarGzippedFile(context, sourceDirectoryContainingShardingFiles, destinationDirectory,
          SHARDING_ZIP_FILE_NAME);
      updateCoordinationServerForShardingTableAvailability(nodes, randomNode, distributedFilePath);
    } catch (IOException | JAXBException | HungryHippoException exception) {
      throw new RuntimeException(exception);
    }
  }

  private void updateCoordinationServerForShardingTableAvailability(List<Node> nodes,
      Node nodeToUploadShardingTableTo, String distributedFilePath)
      throws FileNotFoundException, JAXBException, IOException, HungryHippoException {
    int nodeIdShardingTableCopiedTo = nodeToUploadShardingTableTo.getIdentifier();
    String shardingTableCopiedOnPath = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + distributedFilePath
        + SHARDING_TABLE_AVAILABLE_WITH_NODE_PATH + nodeIdShardingTableCopiedTo;
    HungryHippoCurator curator = HungryHippoCurator.getInstance();
    curator.createPersistentNode(
        shardingTableCopiedOnPath + nodeIdShardingTableCopiedTo, new CountDownLatch(1));
    String shardingTableToBeCopiedOnNodePath =
        CoordinationConfigUtil.getZkCoordinationConfigCache().getZookeeperDefaultConfig()
            .getFilesystemPath() + distributedFilePath + SHARDING_TABLE_TO_BE_COPIED_ON_NODE_PATH;
    nodes.stream().filter(node -> node.getIdentifier() != nodeIdShardingTableCopiedTo)
        .forEach(node -> {
          try {
            curator.createPersistentNode(
                shardingTableToBeCopiedOnNodePath + node.getIdentifier(), new CountDownLatch(1));
          } catch (HungryHippoException exception) {
            throw new RuntimeException(exception);
          }
        });
  }

}
