package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.util.List;
import java.util.Random;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.utility.scp.Jscp;
import com.talentica.hungryHippos.utility.scp.SecureContext;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.config.sharding.Output;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * Utility class to copy generated sharding tables on nodes/local file system.
 * 
 * @author nitink
 *
 */
public class ShardingTableCopier {

  private static final Random RANDOM = new Random();

  private static final String SHARDING_ZIP_FILE_NAME = "sharding-table";

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
   */
  public void copyToAnyRandomNodeInCluster() {
    String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
    List<Node> nodes = CoordinationApplicationContext.getZkClusterConfigCache().getNode();
    int totalNoOfNodes = nodes.size();
    Node nodeToUploadShardingTableTo = nodes.get(RANDOM.nextInt(totalNoOfNodes));
    String nodeSshUsername = outputConfiguration.getNodeSshUsername();
    String nodeSshPrivateKeyFilePath = outputConfiguration.getNodeSshPrivateKeyFilePath();
    File privateKeyFile = new File(nodeSshPrivateKeyFilePath);
    String host = nodeToUploadShardingTableTo.getIp();
    String destinationDirectory = fileSystemBaseDirectory + File.separator
        + shardingClientConfig.getInput().getDistributedFilePath();
    SecureContext context = new SecureContext(nodeSshUsername, host);
    context.setPrivateKeyFile(privateKeyFile);
    Jscp.scpTarGzippedFile(context, sourceDirectoryContainingShardingFiles, destinationDirectory,
        SHARDING_ZIP_FILE_NAME);
  }

}
