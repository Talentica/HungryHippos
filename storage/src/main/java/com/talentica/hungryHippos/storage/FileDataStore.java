package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileDataStore implements DataStore, Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -7726551156576482829L;
  private static final Logger logger = LoggerFactory.getLogger(FileDataStore.class);
  private final int numFiles;
  private FileOutputStream[] fos;
  private DataDescription dataDescription;
  private String hungryHippoFilePath;
  private String fileNamePrefix;
  private String nodeId;
 private ShardingApplicationContext context;
  private static final boolean APPEND_TO_DATA_FILES = FileSystemContext.isAppendToDataFile();

  private transient Map<Integer, FileStoreAccess> primaryDimensionToStoreAccessCache =
      new HashMap<>();

  public String DATA_FILE_BASE_NAME = FileSystemContext.getDataFilePrefix();

  public FileDataStore(int numDimensions, DataDescription dataDescription,
      String hungryHippoFilePath, String nodeId, ShardingApplicationContext context) throws IOException, InterruptedException,
      ClassNotFoundException, KeeperException, JAXBException {
    this(numDimensions, dataDescription, hungryHippoFilePath, nodeId, false,context);
  }

  public FileDataStore(int numDimensions, DataDescription dataDescription,
      String hungryHippoFilePath, String nodeId, boolean readOnly, ShardingApplicationContext context) throws IOException {
    this.context = context;
    this.numFiles = 1 << numDimensions;
    this.dataDescription = dataDescription;
    fos = new FileOutputStream[numFiles];
    this.nodeId = nodeId;
    this.hungryHippoFilePath = hungryHippoFilePath;
    String dirloc = FileSystemContext.getRootDirectory() + hungryHippoFilePath;
    this.fileNamePrefix = dirloc + File.separator + DATA_FILE_BASE_NAME;
    File file = new File(dirloc);
    if (!file.exists()) {
      boolean flag = file.mkdir();
      if (flag) {
        logger.info("created data folder");
      } else {
        logger.info("Not able to create dataFolder");
      }
    }
    if (!readOnly) {
      for (int i = 0; i < numFiles; i++) {
        fos[i] =
            new FileOutputStream(fileNamePrefix + i, APPEND_TO_DATA_FILES);
      }
    }
  }

  @Override
  public void storeRow(int storeId, ByteBuffer row, byte[] raw) {
      FileLock fileLock = null;
    try {
        fileLock = fos[storeId].getChannel().lock();
        fos[storeId].write(raw);
        fos[storeId].flush();
    } catch (IOException e) {
      logger.error("Error occurred while writing data received to datastore.", e);
    }
      finally {
        if(fileLock!=null){
            try {
                fileLock.release();
            } catch (IOException e) {
               logger.error("Error occurred while releasing fileLock.", e);
                throw new RuntimeException(e);
            }
        }
    }
  }

  @Override
  public StoreAccess getStoreAccess(int keyId) throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    int shardingIndexSequence = context.getShardingIndexSequence(keyId);
    FileStoreAccess storeAccess = primaryDimensionToStoreAccessCache.get(shardingIndexSequence);
    if (storeAccess == null) {
      storeAccess = new FileStoreAccess(hungryHippoFilePath, DATA_FILE_BASE_NAME,
          shardingIndexSequence, numFiles, dataDescription);
      primaryDimensionToStoreAccessCache.put(keyId, storeAccess);
    }
    storeAccess.clear();
    return storeAccess;
  }

  @Override
  public void sync() {
    for (int i = 0; i < numFiles; i++) {
      try {
        fos[i].flush();
      } catch (IOException e) {
        logger.error("Error occurred while flushing " + i + "th outputstream.", e);
      } finally {
        try {
          if (fos[i] != null)
            fos[i].close();
          HungryHipposFileSystem.getInstance().updateFSBlockMetaData(hungryHippoFilePath, nodeId,
              i + "", (new File(fileNamePrefix + i)).length());
        } catch (IOException e) {
          logger.warn("\n\tUnable to close the connection; exception :: " + e.getMessage());
        } catch (Exception e) {
          logger.error(e.toString());
        }
      }
    }
  }


    public String getHungryHippoFilePath() {
        return hungryHippoFilePath;
    }


}
