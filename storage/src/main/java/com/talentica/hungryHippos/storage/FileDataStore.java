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

import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

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
  private String hungryHippoFilePath;
  private String fileNamePrefix;
  private String nodeId;

  private static final boolean APPEND_TO_DATA_FILES = true/*Boolean
      .valueOf(CoordinationApplicationContext.getZkCoordinationConfigCache().getNodeConfig()
          .isDatareceiverAppendToDataFiles())*/;

  private transient Map<Integer, FileStoreAccess> primaryDimensionToStoreAccessCache =
      new HashMap<>();

 public String DATA_FILE_BASE_NAME = FileSystemContext.getDataFilePrefix();

  public FileDataStore(int numDimensions, DataDescription dataDescription, String hungryHippoFilePath,String nodeId) throws IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
    this(numDimensions, dataDescription, hungryHippoFilePath,nodeId, false);
  }

  public FileDataStore(int numDimensions, DataDescription dataDescription, String hungryHippoFilePath, String nodeId,boolean readOnly)
          throws IOException{
    this.numFiles = 1 << numDimensions;
    this.dataDescription = dataDescription;
    os = new OutputStream[numFiles];
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
        os[i] = new BufferedOutputStream(new FileOutputStream(fileNamePrefix + i, APPEND_TO_DATA_FILES));
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
    int shardingIndexSequence = ShardingApplicationContext.getShardingIndexSequence(keyId,hungryHippoFilePath);
    FileStoreAccess storeAccess = primaryDimensionToStoreAccessCache.get(shardingIndexSequence);
    if (storeAccess == null) {
      storeAccess =
          new FileStoreAccess(hungryHippoFilePath, DATA_FILE_BASE_NAME, shardingIndexSequence, numFiles, dataDescription);
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
          HungryHipposFileSystem.getInstance().updateFSBlockMetaData(hungryHippoFilePath, nodeId,i+"",
                  (new File(fileNamePrefix+i)).length() );
        } catch (IOException e) {
          logger.warn("\n\tUnable to close the connection; exception :: " + e.getMessage());
        } catch (Exception e){
          logger.error(e.toString());
        }
      }
    }
  }

}
