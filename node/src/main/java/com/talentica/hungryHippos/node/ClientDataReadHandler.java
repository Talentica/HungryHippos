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
    private DataDescription dataDescription;
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
    private int dataSize;
    private Map<Integer,ReplicaDataSender> replicaDataSenders;
    private Map<Integer,byte[][]> memoryBlocks;
    //private byte[][][] memoryBlocks;
    private Map<Integer,Integer> currentMemoryBlockId;
    private Map<Integer,Integer> blockRecordCount;
    private int maxNoOfRecords;

    public ClientDataReadHandler(DataDescription dataDescription, DataStore dataStore,
                                 byte[] remainingBufferData, NodeUtil nodeUtil, ShardingApplicationContext context)
            throws IOException {
        LOGGER.info("Inside ClientDataReadHandler Constructor");
        this.previousHandlerUnprocessedData = remainingBufferData;
        this.dataDescription = dataDescription;
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
        dataSize = dataDescription.getSize();
        nextNodesInfo = new byte[replicaNodesInfoDataSize];
        maxNoOfRecords = DataPublisherApplicationContext.getMaxRecordBufferSize();
        memoryBlocks = new HashMap<Integer,byte[][]>();
        //memoryBlocks = new byte[nodes.size()][20][];
        blockRecordCount = new HashMap<Integer,Integer>();
        currentMemoryBlockId = new HashMap<Integer,Integer>();
        for (Node node : nodes) {
            String nodeIp = node.getIp();
            
            if (!nodeIp.equals(NodeInfo.INSTANCE.getIp())) {
                int nodeId = node.getIdentifier();
                int port = Integer.parseInt(node.getPort());
                blockRecordCount.put(nodeId, 0);
                byte[][] nodeMemoryBlocks = new byte[20][];
                memoryBlocks.put(nodeId, nodeMemoryBlocks);
                for (int j = 0; j < 20; j++) {
                    memoryBlocks.get(nodeId)[j] = new byte[dataSize * maxNoOfRecords];
                }
                currentMemoryBlockId.put(nodeId, 0);
                replicaDataSenders.put(nodeId, new ReplicaDataSender(nodeIp, port, dataStore.getHungryHippoFilePath(), memoryBlocks.get(nodeId)));
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
            replicaDataSenders.get(nodeId).publishRemainingReplicaData(blockRecordCount.get(nodeId)*dataSize, currentMemoryBlockId.get(nodeId));
            replicaDataSenders.get(nodeId).closeConnection();
            replicaDataSenders.get(nodeId).clearReferences();
        }
        }
        dataStore.sync();
        byteBuf.release();
        byteBuf = null;
        ctx.channel().close();
        nextNodesInfo = null;
        memoryBlocks = null;
        currentMemoryBlockId = null;
        blockRecordCount= null;
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
                int nodeId = (int) nextNodesInfo[i];
                System.arraycopy(buf, 0, memoryBlocks.get(nodeId)[currentMemoryBlockId.get(nodeId)], blockRecordCount.get(nodeId) * dataSize, dataSize);
                blockRecordCount.put(nodeId, blockRecordCount.get(nodeId) + 1);
                if (blockRecordCount.get(nodeId) == maxNoOfRecords) {
                    switchBlock(nodeId);
                }
            }
            int storeId = nodeDataStoreIdCalculator.storeId(byteBuffer);
            dataStore.storeRow(storeId, buf);
        }

    }

    private void switchBlock(int nodeId){
        replicaDataSenders.get(nodeId).setMemoryBlockStatus(ReplicaDataSender.Status.ENABLE_BLOCK_READ, currentMemoryBlockId.get(nodeId));
        currentMemoryBlockId.put(nodeId, -1);
        while (currentMemoryBlockId.get(nodeId)==-1) {
            for (int j = 0; j < memoryBlocks.get(nodeId).length; j++) {
                if(replicaDataSenders.get(nodeId).getMemoryBlockStatus(j) == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE){
                    currentMemoryBlockId.put(nodeId, j);
                    break;
                }
            }
        }
        blockRecordCount.put(nodeId, 0);
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
