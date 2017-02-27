package com.talentica.hungryHippos.node.service;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;

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

      OutputStream bos = new BufferedOutputStream(new FileOutputStream(destinationPath));
      int read = 0;
      while (size != 0) {
        read = dis.read(buffer);
        bos.write(buffer, 0, read);
        size -= read;
      }

      bos.flush();
      bos.close();
      logger.debug("accepted all the data send");
      dos.writeBoolean(true);
      dos.flush();
      if(fileType == HungryHippoServicesConstants.SHARDING_TABLE){
        String shardingTableFolderPath = destinationPath.substring(0, destinationPath.lastIndexOf(File.separatorChar))
            + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        NewDataHandler.updateFilesIfRequired(shardingTableFolderPath);
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
      try {
        FileUtils.deleteDirectory(parentDir.toFile());
        dos.writeBoolean(false);
        dos.flush();

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

}
