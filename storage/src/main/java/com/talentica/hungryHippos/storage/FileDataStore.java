package com.talentica.hungryHippos.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.sorting.DataFileSorter;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileDataStore implements DataStore {
  /**
   * 
   */
  private static final Logger logger = LoggerFactory.getLogger(FileDataStore.class);
  private final int numFiles;
  private OutputStream[] os;
  private DataDescription dataDescription;
  private String hungryHippoFilePath;
  private int nodeId;
  private ShardingApplicationContext context;
  private static final boolean APPEND_TO_DATA_FILES = FileSystemContext.isAppendToDataFile();
  private String uniqueFileName;
  private String dataFilePrefix;

  private transient Map<Integer, FileStoreAccess> primaryDimensionToStoreAccessCache =
      new HashMap<>();

  public String DATA_FILE_BASE_NAME = FileSystemContext.getDataFilePrefix();

  public FileDataStore(int numDimensions, DataDescription dataDescription,

      String hungryHippoFilePath, String nodeId, ShardingApplicationContext context,
      String fileName) throws IOException, InterruptedException, ClassNotFoundException,
      KeeperException, JAXBException {
    this(numDimensions, dataDescription, hungryHippoFilePath, nodeId, false, context, fileName);
  }

  public FileDataStore(int numDimensions, DataDescription dataDescription,
      String hungryHippoFilePath, String nodeId, boolean readOnly,
      ShardingApplicationContext context, String fileName) throws IOException {
    this.context = context;
    this.numFiles = 1 << numDimensions;
    this.dataDescription = dataDescription;
    os = new OutputStream[numFiles];
    this.nodeId = Integer.parseInt(nodeId);
    this.hungryHippoFilePath = hungryHippoFilePath;
    this.dataFilePrefix = FileSystemContext.getRootDirectory() + hungryHippoFilePath
        + File.separator + DATA_FILE_BASE_NAME;
    this.uniqueFileName = fileName;
    if (!readOnly) {
      for (int i = 0; i < numFiles; i++) {
        String filePath = dataFilePrefix + i + "/" + uniqueFileName;
        File file = new File(filePath);
        if (!file.getParentFile().exists()) {
          boolean flag = file.getParentFile().mkdirs();
          if (flag) {
            logger.info("created data folder");
          } else {
            logger.info("Not able to create dataFolder");
          }
        }
        os[i] = new FileOutputStream(filePath, APPEND_TO_DATA_FILES);
      }
    }
  }

  public FileDataStore(int numDimensions, DataDescription dataDescription,
      String hungryHippoFilePath, String nodeId, boolean readOnly,
      ShardingApplicationContext context) throws IOException {
    this(numDimensions, dataDescription, hungryHippoFilePath, nodeId, readOnly, context,
        "<fileName>");
  }

  @Override
  public void storeRow(int storeId, byte[] raw) {
    try {
      os[storeId].write(raw);
    } catch (IOException e) {
      logger.error("Error occurred while writing data received to datastore.", e);
    }
  }

  @Override
  public StoreAccess getStoreAccess(int keyId) throws ClassNotFoundException, KeeperException,
      InterruptedException, IOException, JAXBException {
    int shardingIndexSequence = context.getShardingIndexSequence(keyId);
    FileStoreAccess storeAccess = primaryDimensionToStoreAccessCache.get(shardingIndexSequence);
    if (storeAccess == null) {
      storeAccess = new FileStoreAccess(hungryHippoFilePath, DATA_FILE_BASE_NAME,
          shardingIndexSequence, numFiles, dataDescription);
      primaryDimensionToStoreAccessCache.put(keyId, storeAccess);
    }
    storeAccess.reset();
    return storeAccess;
  }

  @Override
  public void sync() {
    for (int i = 0; i < numFiles; i++) {
      try {
        os[i].flush();
      } catch (IOException e) {
        logger.error("Error occurred while flushing " + i + "th outputstream.", e);
      } finally {
        try {
          if (os[i] != null)
            os[i].close();
          HungryHipposFileSystem.getInstance().updateFSBlockMetaData(hungryHippoFilePath, nodeId, i,
              uniqueFileName,
              (new File(dataFilePrefix + i + File.separatorChar + uniqueFileName)).length());
        } catch (IOException e) {
          logger.warn("\n\tUnable to close the connection; exception :: " + e.getMessage());
        } catch (Exception e) {
          logger.error(e.toString());
        }
      }
    }
  }

  @Override
  public String getHungryHippoFilePath() {
    return hungryHippoFilePath;
  }


}
