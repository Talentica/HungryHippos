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
package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.datareceiver.ApplicationCache;
import com.talentica.hungryHippos.node.datareceiver.HHFileNamesIdentifier;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class AcceptFileService implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(AcceptFileService.class);
  private Socket socket;

  public AcceptFileService(Socket socket) {
    this.socket = socket;
  }

  @Override
  public void run() {
    DataInputStream dis = null;
    DataOutputStream dos = null;
    Path parentDir = null;
    try {
      dis = new DataInputStream(this.socket.getInputStream());
      dos = new DataOutputStream(this.socket.getOutputStream());
      String destinationPath = dis.readUTF();
      String hhFilePath = dis.readUTF();
      long size = dis.readLong();
      int fileType = dis.readInt();
      logger.debug("file size to accept is {}", size);
      int idealBufSize = 8192;
      byte[] buffer = new byte[idealBufSize];
      File file = new File(destinationPath);
      parentDir = Paths.get(file.getParent());
      if (!(Files.exists(parentDir))) {
        logger.debug("created parent folder {}", parentDir);
        Files.createDirectories(parentDir);
      }
      FileOutputStream fos= new FileOutputStream(destinationPath);

      OutputStream bos = new BufferedOutputStream(fos);
      int read = 0;
      while (size != 0) {
        read = dis.read(buffer);
        bos.write(buffer, 0, read);
        size -= read;
      }

      bos.flush();
      fos.flush();
      bos.close();
      fos.close();
      logger.debug("accepted all the data send");
      dos.writeBoolean(true);
      dos.flush();
      if(fileType == HungryHippoServicesConstants.SHARDING_TABLE){
        String shardingTableFolderPath = destinationPath.substring(0, destinationPath.lastIndexOf(File.separatorChar))
            + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        updateFilesIfRequired(shardingTableFolderPath);
        createFolders(FileSystemContext.getRootDirectory()+hhFilePath+File.separator);
        ShardingApplicationContext context = new ShardingApplicationContext(shardingTableFolderPath);
        HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> keyToBucketToNodeMap =
                ShardingFileUtil.readFromFileBucketToNodeNumber(context.getBuckettoNodeNumberMapFilePath());
        Map<String,int[]> fileToNodeIdMap = new HHFileNamesIdentifier(context.getShardingDimensions(),
                keyToBucketToNodeMap, Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize())).getFileToNodeMap();
        try(FileOutputStream fileOutputStream = new FileOutputStream(FileSystemContext.getRootDirectory()+hhFilePath+File.separator+FileSystemConstants.FILE_LOCATION_INFO);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedOutputStream)){
          objectOutputStream.writeObject(fileToNodeIdMap);
          objectOutputStream.flush();
          bufferedOutputStream.flush();
        }
      }
    } catch (IOException | JAXBException e) {
      logger.error(e.getMessage());
      try {
        if(parentDir!=null) {
          FileUtils.deleteDirectory(parentDir.toFile());
        }
        if(dos!=null) {
          dos.writeBoolean(false);
          dos.flush();
        }

      } catch (IOException e1) {
        logger.error(e1.getMessage());
      }

    } finally {
      if (this.socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
      }
    }
  }

  private void createFolders(String filePath) {
    new File(filePath+ FileSystemConstants.BLOCK_STATISTICS_FOLDER_NAME).mkdir();
    new File(filePath+ FileSystemConstants.FILE_STATISTICS_FOLDER_NAME).mkdir();
    new File(filePath+ FileSystemConstants.META_DATA_FOLDER_NAME).mkdir();
    new File(filePath+ FileSystemContext.getDataFilePrefix()).mkdir();
  }

  /**
   * Updates the sharding files if required
   *
   * @param shardingTableFolderPath
   * @throws IOException
   */
  public static void updateFilesIfRequired(String shardingTableFolderPath) throws IOException {
    String shardingTableZipPath = shardingTableFolderPath + ".tar.gz";
    File shardingTableFolder = new File(shardingTableFolderPath);
    File shardingTableZip = new File(shardingTableZipPath);
    if (shardingTableFolder.exists()) {
      if (shardingTableFolder.lastModified() < shardingTableZip.lastModified()) {
        FileSystemUtils.deleteFilesRecursively(shardingTableFolder);
        TarAndGzip.untarTGzFile(shardingTableZipPath);
      }
    } else {
      TarAndGzip.untarTGzFile(shardingTableZipPath);
    }
  }

}
