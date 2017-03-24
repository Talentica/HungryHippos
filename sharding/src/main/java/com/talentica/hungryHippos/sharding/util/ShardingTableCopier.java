/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.talentica.hungryHippos.utility.scp.TarAndGzip;
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

  private ShardingClientConfig shardingClientConfig;
  private String sourceDirectoryContainingShardingFiles;

  public ShardingTableCopier(String sourceDirectoryContainingShardingFiles,
      ShardingClientConfig shardingClientConfig) {
    this.shardingClientConfig = shardingClientConfig;
    this.sourceDirectoryContainingShardingFiles = sourceDirectoryContainingShardingFiles;
  }


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
      String destinationDirectory = fileSystemBaseDirectory + distributedFilePath
          + File.separatorChar + SHARDING_ZIP_FILE_NAME + ".tar.gz";


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

    } catch (IOException e) {

      e.printStackTrace();
    }


  }



}


