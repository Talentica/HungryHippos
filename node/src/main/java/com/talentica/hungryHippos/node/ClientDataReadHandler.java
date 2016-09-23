package com.talentica.hungryHippos.node;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.context.DataPublisherApplicationContext;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryhippos.config.cluster.Node;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * Created by rajkishoreh on 8/9/16.
 */
public class ClientDataReadHandler extends ChannelHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientDataReadHandler.class);
    private byte[] buf;
    private byte[] previousHandlerUnprocessedData;
    private byte[] bufferForReset;
    private ByteBuffer byteBuffer;
    private DataStore dataStore;
    private ByteBuf byteBuf;
    private NodeDataStoreIdCalculator nodeDataStoreIdCalculator;
    private int recordSize;
    private byte[] nextNodesInfo;
    private int replicaNodesInfoDataSize;
    private Map<Integer,ReplicaDataSender> replicaDataSenders;
    private Map<Integer,byte[][]> nodeIdToMemoryArrayBlocksMap;
    private Map<Integer,Integer> nodeIdToCurrentMemoryBlockMap;
    private Map<Integer,Integer> nodeIdToCurrentMemoryBlockIndexMap;
    private int memoryBlockCapacity;

    public ClientDataReadHandler(DataDescription dataDescription, DataStore dataStore,
                                 byte[] remainingBufferData, NodeUtil nodeUtil, ShardingApplicationContext context)
            throws IOException {
        LOGGER.info("Inside ClientDataReadHandler Constructor");
        this.previousHandlerUnprocessedData = remainingBufferData;
        this.buf = new byte[dataDescription.getSize()];
        byteBuffer = ByteBuffer.wrap(this.buf);
        this.dataStore = dataStore;
        int shardingDimensions = context.getShardingDimensions().length;
        replicaNodesInfoDataSize = shardingDimensions - 1;
        recordSize = replicaNodesInfoDataSize + dataDescription.getSize();
        bufferForReset = new byte[recordSize];
        nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(nodeUtil.getKeyToValueToBucketMap(),
                nodeUtil.getBucketToNodeNumberMap(), NodeInfo.INSTANCE.getIdentifier(), dataDescription, context);
        List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        replicaDataSenders = new HashMap<Integer,ReplicaDataSender>();
        nextNodesInfo = new byte[replicaNodesInfoDataSize];
        int maxNoOfRecords = DataPublisherApplicationContext.getMaxRecordBufferSize();
        memoryBlockCapacity = maxNoOfRecords*recordSize;
        nodeIdToMemoryArrayBlocksMap = new HashMap<Integer,byte[][]>();
        nodeIdToCurrentMemoryBlockIndexMap = new HashMap<Integer,Integer>();
        nodeIdToCurrentMemoryBlockMap = new HashMap<Integer,Integer>();
        int memoryArraySize = 10;
        for (Node node : nodes) {
            String nodeIp = node.getIp();
            if (!nodeIp.equals(NodeInfo.INSTANCE.getIp())) {
                int nodeId = node.getIdentifier();
                int port = Integer.parseInt(node.getPort());
                nodeIdToCurrentMemoryBlockIndexMap.put(nodeId, 0);
                byte[][] memoryArrayBlock = new byte[memoryArraySize][];
                nodeIdToMemoryArrayBlocksMap.put(nodeId, memoryArrayBlock);
                for (int j = 0; j < memoryArraySize; j++) {
                    nodeIdToMemoryArrayBlocksMap.get(nodeId)[j] = new byte[memoryBlockCapacity];
                }
                nodeIdToCurrentMemoryBlockMap.put(nodeId, 0);
                replicaDataSenders.put(nodeId, new ReplicaDataSender(nodeIp, port, dataStore.getHungryHippoFilePath(), nodeIdToMemoryArrayBlocksMap.get(nodeId)));
                replicaDataSenders.get(nodeId).start();
            }
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws IOException, InterruptedException {
        byteBuf = ctx.alloc().buffer(recordSize * 20);
        byteBuf.writeBytes(previousHandlerUnprocessedData);
        previousHandlerUnprocessedData = null;
        processData();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws InterruptedException, IOException {
        LOGGER.info("Inside handlerRemoved");
        writeDataInStore();
        for(int nodeId : replicaDataSenders.keySet()){
          if (replicaDataSenders.get(nodeId) != null) {
            replicaDataSenders.get(nodeId).kill();
            replicaDataSenders.get(nodeId).join();
            replicaDataSenders.get(nodeId).publishRemainingReplicaData(nodeIdToCurrentMemoryBlockIndexMap.get(nodeId), nodeIdToCurrentMemoryBlockMap.get(nodeId));
            replicaDataSenders.get(nodeId).closeConnection();
            replicaDataSenders.get(nodeId).clearReferences();
        }
        }
        dataStore.sync();
        byteBuf.release();
        byteBuf = null;
        ctx.channel().close();
        nextNodesInfo = null;
        nodeIdToMemoryArrayBlocksMap = null;
        nodeIdToCurrentMemoryBlockMap = null;
        nodeIdToCurrentMemoryBlockIndexMap = null;
        LOGGER.info("Exiting handlerRemoved");
    }

    private void waitForDataPublishersServerConnectRetryInterval() throws InterruptedException {
        // TODO NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE value should be discussed.
        int NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE = Integer.valueOf(DataPublisherApplicationContext.getNoOfAttemptsToConnectToNode());
        Thread.sleep(NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE * 2);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException, InterruptedException {
        ByteBuf msgB = (ByteBuf) msg;
        byteBuf.writeBytes(msgB);
        msgB.release();
        processData();
    }

    /**
     * Writes the data in DataStore and reInitializes byteBuf
     */
    private void processData() throws IOException, InterruptedException {
        writeDataInStore();
        reInitializeByteBuf();
    }

    /**
     * Writes the data in DataStore
     */
    private void writeDataInStore() {

        while (byteBuf.readableBytes() >= recordSize) {
            byteBuf.readBytes(nextNodesInfo);
            byteBuf.readBytes(buf);
            for (int i = 0; i < replicaNodesInfoDataSize; i++) {
                if(nextNodesInfo[i]!=(byte)-1){
                    int nodeId = (int) nextNodesInfo[i];
                    int currentIndex = nodeIdToCurrentMemoryBlockIndexMap.get(nodeId);
                    byte[] currentMemoryBlock = nodeIdToMemoryArrayBlocksMap.get(nodeId)[nodeIdToCurrentMemoryBlockMap.get(nodeId)];
                    for (int j = 0; j <= i; j++) {
                        currentMemoryBlock[currentIndex+j] = (byte) -1;
                    }
                    System.arraycopy(nextNodesInfo,i+1,currentMemoryBlock,currentIndex+i+1, replicaNodesInfoDataSize-(i+1));
                    System.arraycopy(buf,0,currentMemoryBlock,currentIndex+replicaNodesInfoDataSize,buf.length);
                    nodeIdToCurrentMemoryBlockIndexMap.put(nodeId, currentIndex + recordSize);
                    if (nodeIdToCurrentMemoryBlockIndexMap.get(nodeId) >= memoryBlockCapacity) {
                        switchMemoryBlock(nodeId);
                    }
                    break;
                }
            }
            int storeId = nodeDataStoreIdCalculator.storeId(byteBuffer);
            dataStore.storeRow(storeId, buf);
        }

    }

    private void switchMemoryBlock(int nodeId){
        replicaDataSenders.get(nodeId).setMemoryBlockStatus(ReplicaDataSender.Status.ENABLE_BLOCK_READ, nodeIdToCurrentMemoryBlockMap.get(nodeId));
        nodeIdToCurrentMemoryBlockMap.put(nodeId, null);
        while (nodeIdToCurrentMemoryBlockMap.get(nodeId)==null) {
            for (int j = 0; j < nodeIdToMemoryArrayBlocksMap.get(nodeId).length; j++) {
                if(replicaDataSenders.get(nodeId).getMemoryBlockStatus(j) == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE){
                    nodeIdToCurrentMemoryBlockMap.put(nodeId, j);
                    break;
                }
            }
        }
        nodeIdToCurrentMemoryBlockIndexMap.put(nodeId, 0);
    }


    /**
     * reInitializes byteBuf
     */
    private void reInitializeByteBuf() {
        int remainingBytes = byteBuf.readableBytes();
        byteBuf.readBytes(bufferForReset, 0, remainingBytes);
        byteBuf.clear();
        byteBuf.writeBytes(bufferForReset, 0, remainingBytes);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error("Error occurred while processing data in channel handler", cause);
        ctx.close();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        ctx.close(promise);
    }

}
