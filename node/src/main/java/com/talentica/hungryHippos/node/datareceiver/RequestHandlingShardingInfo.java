package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.NodeUtil;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by rajkishoreh on 28/9/16.
 */
public class RequestHandlingShardingInfo {
    private ShardingApplicationContext context;
    private FieldTypeArrayDataDescription dataDescription;
    private NodeUtil nodeUtil;
    private int replicaNodesInfoDataSize;
    private int recordSize;
    private byte[] fileIdInBytes;


    public RequestHandlingShardingInfo(int fileId, String hhFilePath) throws IOException {
        fileIdInBytes = ByteBuffer.allocate(DataHandler.FILE_ID_BYTE_SIZE).putInt(fileId).array();
        String dataAbsolutePath = FileSystemContext.getRootDirectory() + hhFilePath;
        String shardingTableFolderPath =
                dataAbsolutePath + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        updateFilesIfRequired(shardingTableFolderPath);
        context = new ShardingApplicationContext(shardingTableFolderPath);
        dataDescription = context.getConfiguredDataDescription();
        dataDescription.setKeyOrder(context.getShardingDimensions());
        nodeUtil = new NodeUtil(hhFilePath);
        int shardingDimensions = context.getShardingDimensions().length;
        replicaNodesInfoDataSize = shardingDimensions - 1;
        recordSize = replicaNodesInfoDataSize + dataDescription.getSize();
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

    public ShardingApplicationContext getContext() {
        return context;
    }

    public FieldTypeArrayDataDescription getDataDescription() {
        return dataDescription;
    }

    public NodeUtil getNodeUtil() {
        return nodeUtil;
    }

    public int getReplicaNodesInfoDataSize() {
        return replicaNodesInfoDataSize;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public byte[] getFileIdInBytes() {
        return fileIdInBytes;
    }
}