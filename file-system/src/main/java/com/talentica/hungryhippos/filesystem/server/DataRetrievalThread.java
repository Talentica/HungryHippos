package com.talentica.hungryhippos.filesystem.server;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * {@code DataRetrievalThread} for creating threads for handling each client request for Data
 * Retrieval.
 * 
 * @author rajkishoreh
 * @since 30/6/16.
 */
public class DataRetrievalThread extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataRetrievalThread.class);

  private Socket clientSocket = null;
  private DataInputStream dis = null;
  private DataOutputStream dos = null;
  private String rootDirectory;
  private int fileStreamBufferSize;

  /**
   * creates a new instance of DataRetrievalThread.
   * 
   * @param clientSocket
   * @param rootDirectory
   * @param fileStreamBufferSize
   */
  public DataRetrievalThread(Socket clientSocket, String rootDirectory, int fileStreamBufferSize) {
    this.clientSocket = clientSocket;
    this.rootDirectory = rootDirectory;
    this.fileStreamBufferSize = fileStreamBufferSize;
    LOGGER.info("[{}] Just connected to {}", Thread.currentThread().getName(),
        clientSocket.getRemoteSocketAddress());
  }


  public void run() {
    try {
      dis = new DataInputStream(clientSocket.getInputStream());
      dos = new DataOutputStream(clientSocket.getOutputStream());
      dos.writeUTF(FileSystemConstants.DATA_SERVER_AVAILABLE);
      String hungryHippoFilePath = dis.readUTF();
      int dimensionOperand = dis.readInt();
      int nodeId = dis.readInt();
      long offset = dis.readLong();
      LOGGER.info("[{}] filePath {}", Thread.currentThread().getName(), hungryHippoFilePath);
      LOGGER.info("[{}] offset {}", Thread.currentThread().getName(), offset);
      byte[] inputBuffer = new byte[fileStreamBufferSize];
      if (dimensionOperand > -1) {
        handleShardedFileDownload(hungryHippoFilePath, dimensionOperand, nodeId, offset,
            inputBuffer);
      } else {
        handleFileDownload(hungryHippoFilePath, offset, inputBuffer);
      }
      dos.flush();
      Thread.sleep(3000);
      dos.writeUTF(FileSystemConstants.DATA_TRANSFER_COMPLETED);
      LOGGER.info("[{}] {}", Thread.currentThread().getName(),
          FileSystemConstants.DATA_TRANSFER_COMPLETED);
    } catch (IOException e) {
      LOGGER.error(e.toString());
    } catch (InterruptedException e) {
      LOGGER.error(e.toString());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
        if (dis != null) {
          dis.close();
        }
        if (dos != null) {
          dos.close();
        }
      } catch (IOException e) {
        LOGGER.error(e.toString());
      }

    }
  }



  private void handleFileDownload(String hungryHippoFilePath, long offset, byte[] inputBuffer)
      throws IOException {
    int len;
    RandomAccessFile raf = new RandomAccessFile(rootDirectory + hungryHippoFilePath, "r");
    if (offset < raf.length()) {
      raf.seek(offset);
      while ((len = raf.read(inputBuffer)) > -1) {
        dos.write(inputBuffer, 0, len);
      }
    }
    raf.close();
  }

  private void handleShardedFileDownload(String hungryHippoFilePath, int dimensionOperand,
      int nodeId, long offset, byte[] inputBuffer) throws IOException {
    int len;
    String fsRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath();
    String fileNodeZKDFSPath = fsRootNode + hungryHippoFilePath
        + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE;
    HungryHippoCurator curator = HungryHippoCurator.getInstance();
    List<String> nodeIds;
    try {
      nodeIds = curator.getChildren(fileNodeZKDFSPath);

      Set<String> nodeIdSet = new HashSet<>();
      nodeIdSet.addAll(nodeIds);
      String nodeIdZKPath = fileNodeZKDFSPath + FileSystemConstants.ZK_PATH_SEPARATOR + nodeId;
      List<String> dataFolderList = curator.getChildren(nodeIdZKPath);
      Set<String> dataFolderSet = new HashSet<>();
      dataFolderSet.addAll(dataFolderList);
      String dataFilePrefix = FileSystemContext.getDataFilePrefix();
      for (String dataFolder : dataFolderSet) {
        int dataFolderIntVal = Integer.parseInt(dataFolder);
        if ((dataFolderIntVal & dimensionOperand) == dimensionOperand) {
          for (String dataFileName : nodeIdSet) {
            File dataFile = new File(rootDirectory + hungryHippoFilePath + File.separatorChar
                + dataFilePrefix + dataFolder + File.separatorChar + dataFileName);
            long dataFileSize = dataFile.length();
            if (dataFileSize > 0) {
              if (offset == 0) {
                BufferedInputStream bis =
                    new BufferedInputStream(new FileInputStream(dataFile.getAbsolutePath()));
                while ((len = bis.read(inputBuffer)) > -1) {
                  dos.write(inputBuffer, 0, len);
                }
                bis.close();
              } else if (offset < dataFileSize) {
                RandomAccessFile raf = new RandomAccessFile(dataFile.getAbsolutePath(), "r");
                if (offset < raf.length()) {
                  raf.seek(offset);
                  while ((len = raf.read(inputBuffer)) > -1) {
                    dos.write(inputBuffer, 0, len);
                  }
                }
                raf.close();
                offset = 0;
              } else {
                offset -= dataFileSize;
              }
            }
          }
        }
      }
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
