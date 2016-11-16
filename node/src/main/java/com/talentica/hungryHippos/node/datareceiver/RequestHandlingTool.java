package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.NodeUtil;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code RequestHandlingTool}  used for handling client request.
 * @author rajkishoreh
 * @since 22/9/16.
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
    private ShardingApplicationContext context;

    /**
     * creates an instance of RequestHandlingTool.
     * @param fileId
     * @param hhFilePath
     * @param nodeIdClient
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws KeeperException
     * @throws JAXBException
     */
    public RequestHandlingTool(int fileId,String hhFilePath, int nodeIdClient) throws IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
        this.fileId = fileId;
        this.nodeIdClient = nodeIdClient;
        this.hhFilePath = hhFilePath;
        RequestHandlingShardingInfo requestHandlingShardingInfo = RequestHandlingShardingInfoCache.INSTANCE.get(nodeIdClient,fileId,hhFilePath);
        fileIdInBytes = requestHandlingShardingInfo.getFileIdInBytes();;
        context = requestHandlingShardingInfo.getContext();
        FieldTypeArrayDataDescription dataDescription = requestHandlingShardingInfo.getDataDescription();
        NodeUtil nodeUtil = requestHandlingShardingInfo.getNodeUtil();
        replicaNodesInfoDataSize = requestHandlingShardingInfo.getReplicaNodesInfoDataSize();
        recordSize = requestHandlingShardingInfo.getRecordSize();
         dataStore = requestHandlingShardingInfo.getDataStore();
        nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(nodeUtil.getKeyToValueToBucketMap(),
                nodeUtil.getBucketToNodeNumberMap(), NodeInfo.INSTANCE.getIdentifier(), dataDescription, context);
        if(replicaNodesInfoDataSize>0){
            nextNodesInfo = new byte[replicaNodesInfoDataSize];
        }
        dataForFileWrite = new byte[dataDescription.getSize()];
        byteBuffer = ByteBuffer.wrap(this.dataForFileWrite);
    }

    /**
     * Stores the data row.
     */
    public void storeData(){
        String fileName = nodeDataStoreIdCalculator.storeId(byteBuffer);
        dataStore.storeRow(fileName, dataForFileWrite);
    }

    /**
     * retrieves the record size.
     * @return
     */
    public int getRecordSize() {
        return recordSize;
    }

    /**
     * retrieves replica nodes information.
     * @return
     */
    public int getReplicaNodesInfoDataSize() {
        return replicaNodesInfoDataSize;
    }

    /**
     * retrieves next nodes information.
     * @return
     */
    public byte[] getNextNodesInfo() {
        return nextNodesInfo;
    }

    public byte[] getDataForFileWrite() {
        return dataForFileWrite;
    }

    /**
     * closes all the resource.
     */
    public void close(){
        LOGGER.info("Closing datastore for Node {}: File {}", nodeIdClient, hhFilePath);
        byteBuffer = null;
        nextNodesInfo = null;
        dataForFileWrite =null;
        RequestHandlingShardingInfoCache.INSTANCE.handleRemove(nodeIdClient,fileId);
        RequestHandlersCache.INSTANCE.removeRequestHandlingTool(nodeIdClient, fileId);
    }

    /**
     * retrieves the fileId.
     * @return
     */
    public byte[] getFileIdInBytes() {
        return fileIdInBytes;
    }

    /**
     * retrieves file path.
     * @return
     */
    public String getHhFilePath() {
        return hhFilePath;
    }

    public ShardingApplicationContext getContext(){
      return context;
    }
}
