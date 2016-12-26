package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.NodeUtil;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code RequestHandlingShardingInfo} is used to keep track of changes in the sharding table. if
 * changes are there this class is used for updating the sharding table.
 * 
 * @author rajkishoreh
 * @since 28/9/16.
 */
public class RequestHandlingShardingInfo {
  private ShardingApplicationContext context;
  private FieldTypeArrayDataDescription dataDescription;
  private NodeUtil nodeUtil;
  private int replicaNodesInfoDataSize;
  private int recordSize;
  private byte[] fileIdInBytes;
  private DataStore dataStore;
  private HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap;
  private String[] shardingDimensions;

  /**
   * creates an instance of RequestHandlingShardingInfo.
   * 
   * @param fileId
   * @param hhFilePath
   * @throws IOException
   */
  public RequestHandlingShardingInfo(int fileId, String hhFilePath) throws IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
    fileIdInBytes = ByteBuffer.allocate(DataHandler.FILE_ID_BYTE_SIZE).putInt(fileId).array();
    String dataAbsolutePath = FileSystemContext.getRootDirectory() + hhFilePath;
    String shardingTableFolderPath =
        dataAbsolutePath + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
    updateFilesIfRequired(shardingTableFolderPath);
    context = new ShardingApplicationContext(shardingTableFolderPath);
    dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    nodeUtil = new NodeUtil(hhFilePath);
    replicaNodesInfoDataSize = context.getShardingDimensions().length - 1;
    recordSize = replicaNodesInfoDataSize + dataDescription.getSize();
    keyToValueToBucketMap=nodeUtil.getKeyToValueToBucketMap();
    shardingDimensions = context.getShardingDimensions();
    List<String> fileNames = new ArrayList<>();
    addFileNameToList(fileNames,"", 0);
    /*dataStore = new FileDataStore(fileNames,nodeUtil.getKeyToValueToBucketMap().size(),
            dataDescription, hhFilePath, NodeInfo.INSTANCE.getId(), context, "");*/
  }


  /**
   * Updates the sharding files if required
   *
   * @param shardingTableFolderPath
   * @throws IOException
   */
  private void updateFilesIfRequired(String shardingTableFolderPath) throws IOException {
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

  /**
   * retrieves the {@code ShardingApplicationContext} associated with this.
   * @return
   */
  public ShardingApplicationContext getContext() {
    return context;
  }

  /**
   * retrieves data description.
   * @return
   */
  public FieldTypeArrayDataDescription getDataDescription() {
    return dataDescription;
  }

  /**
   * retrieves the NodeUtil.
   * @return
   */
  public NodeUtil getNodeUtil() {
    return nodeUtil;
  }

  public int getReplicaNodesInfoDataSize() {
    return replicaNodesInfoDataSize;
  }

  /**
   * retrieves the record size.
   * @return
   */
  public int getRecordSize() {
    return recordSize;
  }

  /**
   * retrieves FileId in byte.
   * @return
   */
  public byte[] getFileIdInBytes() {
    return fileIdInBytes;
  }



  private void addFileNameToList(List<String> fileNames, String fileName, int dimension) {
    if (dimension == shardingDimensions.length) {
      fileNames.add(fileName);
      return;
    }
    if(dimension!=0){
      fileName = new String(fileName + "_");
    }
    Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = keyToValueToBucketMap.get(shardingDimensions[dimension]);
    for (Map.Entry<Object, Bucket<KeyValueFrequency>> valueToBucketEntry : valueToBucketMap.entrySet()) {
      addFileNameToList(fileNames, new String(fileName + valueToBucketEntry.getValue().getId()), dimension + 1);
    }
  }

  public DataStore getDataStore() {
    return dataStore;
  }
}
