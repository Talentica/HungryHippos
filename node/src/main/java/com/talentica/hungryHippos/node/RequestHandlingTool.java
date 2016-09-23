package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by rajkishoreh on 22/9/16.
 */
public class RequestHandlingTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestHandlingTool.class);
    private NodeDataStoreIdCalculator nodeDataStoreIdCalculator;
    private int recordSize;
    private int replicaNodesInfoDataSize;
    private DataStore dataStore;
    private byte[] nextNodesInfo;
    private byte[] dataForFileWrite;
    private ByteBuffer byteBuffer;
    private byte[] fileIdInBytes;
    private String nodeFileName;
    private String hhFilePath;

    public RequestHandlingTool(int fileId,String hhFilePath, String nodeFileName) throws IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
        this.nodeFileName = nodeFileName;
        this.hhFilePath = hhFilePath;
        fileIdInBytes = ByteBuffer.allocate(DataHandler.INT_BYTE_SIZE).putInt(fileId).array();
        LOGGER.info("File :{}",hhFilePath);
        String dataAbsolutePath = FileSystemContext.getRootDirectory() + hhFilePath;
        String shardingTableFolderPath =
                dataAbsolutePath + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        updateFilesIfRequired(shardingTableFolderPath);
        ShardingApplicationContext context = new ShardingApplicationContext(shardingTableFolderPath);
        FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
        dataDescription.setKeyOrder(context.getShardingDimensions());
        NodeUtil nodeUtil = new NodeUtil(hhFilePath);
        dataStore = new FileDataStore(nodeUtil.getKeyToValueToBucketMap().size(),
                dataDescription, hhFilePath, NodeInfo.INSTANCE.getId(), context, nodeFileName);
        nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(nodeUtil.getKeyToValueToBucketMap(),
                nodeUtil.getBucketToNodeNumberMap(), NodeInfo.INSTANCE.getIdentifier(), dataDescription, context);
        int shardingDimensions = context.getShardingDimensions().length;
        replicaNodesInfoDataSize = shardingDimensions - 1;
        recordSize = replicaNodesInfoDataSize + dataDescription.getSize();
        nextNodesInfo = new byte[replicaNodesInfoDataSize];
        dataForFileWrite = new byte[dataDescription.getSize()];
        byteBuffer = ByteBuffer.wrap(this.dataForFileWrite);
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

    public void storeData(){
        int storeId = nodeDataStoreIdCalculator.storeId(byteBuffer);
        dataStore.storeRow(storeId, dataForFileWrite);
    }

    public int getRecordSize() {
        return recordSize;
    }

    public int getReplicaNodesInfoDataSize() {
        return replicaNodesInfoDataSize;
    }

    public byte[] getNextNodesInfo() {
        return nextNodesInfo;
    }

    public byte[] getDataForFileWrite() {
        return dataForFileWrite;
    }

    public void closeDataStore(){
        LOGGER.info("Closing datastore for Node {}: File {}",nodeFileName, hhFilePath);
        byteBuffer = null;
        nextNodesInfo = null;
        dataForFileWrite =null;
        dataStore.sync();
    }

    public byte[] getFileIdInBytes() {
        return fileIdInBytes;
    }
}
