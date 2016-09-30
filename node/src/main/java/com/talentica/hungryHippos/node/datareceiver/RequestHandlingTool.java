package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.NodeUtil;
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
    private int nodeIdClient;
    private String hhFilePath;
    private int fileId;

    public RequestHandlingTool(int fileId,String hhFilePath, int nodeIdClient) throws IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
        this.fileId = fileId;
        this.nodeIdClient = nodeIdClient;
        this.hhFilePath = hhFilePath;
        RequestHandlingShardingInfo requestHandlingShardingInfo = RequestHandlingShardingInfoCache.INSTANCE.get(nodeIdClient,fileId,hhFilePath);
        fileIdInBytes = requestHandlingShardingInfo.getFileIdInBytes();;
        ShardingApplicationContext context = requestHandlingShardingInfo.getContext();
        FieldTypeArrayDataDescription dataDescription = requestHandlingShardingInfo.getDataDescription();
        NodeUtil nodeUtil = requestHandlingShardingInfo.getNodeUtil();
        replicaNodesInfoDataSize = requestHandlingShardingInfo.getReplicaNodesInfoDataSize();
        recordSize = requestHandlingShardingInfo.getRecordSize();
        dataStore = new FileDataStore(nodeUtil.getKeyToValueToBucketMap().size(),
                dataDescription, hhFilePath, NodeInfo.INSTANCE.getId(), context, nodeIdClient+"");
        nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(nodeUtil.getKeyToValueToBucketMap(),
                nodeUtil.getBucketToNodeNumberMap(), NodeInfo.INSTANCE.getIdentifier(), dataDescription, context);
        nextNodesInfo = new byte[replicaNodesInfoDataSize];
        dataForFileWrite = new byte[dataDescription.getSize()];
        byteBuffer = ByteBuffer.wrap(this.dataForFileWrite);
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

    public void close(){
        LOGGER.info("Closing datastore for Node {}: File {}", nodeIdClient, hhFilePath);
        byteBuffer = null;
        nextNodesInfo = null;
        dataForFileWrite =null;
        dataStore.sync();
        RequestHandlingShardingInfoCache.INSTANCE.handleRemove(nodeIdClient,fileId);
        RequestHandlersCache.INSTANCE.removeRequestHandlingTool(nodeIdClient, fileId);
    }

    public byte[] getFileIdInBytes() {
        return fileIdInBytes;
    }
}
