package com.talentica.hungryHippos.storage;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;

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
  private OutputStream[] os;
  private DataDescription dataDescription;

  private static final boolean APPEND_TO_DATA_FILES = Boolean
      .valueOf(CoordinationApplicationContext.getCoordinationConfig().getNodeConfig()
          .isDatareceiverAppendToDataFiles());

  private transient Map<Integer, FileStoreAccess> primaryDimensionToStoreAccessCache =
      new HashMap<>();

  public static final String DATA_FILE_BASE_NAME = CoordinationApplicationContext
      .getCoordinationConfig().getNodeConfig().getDataStorage().getFileName();

  // public static final String FOLDER_NAME = "data";

  public FileDataStore(int numDimensions, DataDescription dataDescription) throws IOException {
    this(numDimensions, dataDescription, false);
  }

  public FileDataStore(int numDimensions, DataDescription dataDescription, boolean readOnly)
      throws IOException {
    this.numFiles = 1 << numDimensions;
    this.dataDescription = dataDescription;
    os = new OutputStream[numFiles];
    // check data folder exists , if its not present this logic will create a folder named data.
    /*
     * String dirloc = new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + File.separator +
     * FOLDER_NAME;
     */
    String dirloc =
        CoordinationApplicationContext.getCoordinationConfig().getNodeConfig().getDataStorage()
            .getPath();
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
        os[i] =
            new BufferedOutputStream(new FileOutputStream(dirloc + DATA_FILE_BASE_NAME + i,
                APPEND_TO_DATA_FILES));
      }
    }
  }

  @Override
  public void storeRow(int storeId, ByteBuffer row, byte[] raw) {
    try {
      os[storeId].write(raw);
    } catch (IOException e) {
      logger.error("Error occurred while writing data received to datastore.", e);
    }
  }

  @Override
  public StoreAccess getStoreAccess(int keyId) throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    int shardingIndexSequence = CoordinationApplicationContext.getShardingIndexSequence(keyId);
    FileStoreAccess storeAccess = primaryDimensionToStoreAccessCache.get(shardingIndexSequence);
    if (storeAccess == null) {
      storeAccess =
          new FileStoreAccess(DATA_FILE_BASE_NAME, shardingIndexSequence, numFiles, dataDescription);
      primaryDimensionToStoreAccessCache.put(keyId, storeAccess);
    }
    storeAccess.clear();
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
        } catch (IOException e) {
          logger.warn("\n\tUnable to close the connection; exception :: " + e.getMessage());
        }
      }
    }
  }

}
