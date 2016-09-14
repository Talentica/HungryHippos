package com.talentica.hungryHippos.node;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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

    private static int dataReaderHandlerCounter = 0;
    private int dataReaderHandlerId = -1;
    private int recordSize;
    private byte[] nextNodesInfo;
    private int replicaNodesInfoDataSize;
    private int dataSize;
    private ReplicaDataSender[] replicaDataSenders;
    private byte[][][] memoryBlocks;
    private int[] currentMemoryBlockId;
    private int[] blockRecordCount;
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
        dataReaderHandlerId = ++dataReaderHandlerCounter;
        int shardingDimensions = context.getShardingDimensions().length;
        replicaNodesInfoDataSize = shardingDimensions - 1;
        recordSize = replicaNodesInfoDataSize + dataDescription.getSize();
        bufferForReset = new byte[recordSize];
        nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(nodeUtil.getKeyToValueToBucketMap(),
                nodeUtil.getBucketToNodeNumberMap(), NodeInfo.INSTANCE.getIdentifier(), dataDescription, context);
        List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        replicaDataSenders = new ReplicaDataSender[nodes.size()];
        dataSize = dataDescription.getSize();
        nextNodesInfo = new byte[replicaNodesInfoDataSize];
        maxNoOfRecords = DataPublisherApplicationContext.getMaxRecordBufferSize();
        memoryBlocks = new byte[nodes.size()][20][];
        blockRecordCount = new int[nodes.size()];
        currentMemoryBlockId = new int[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            String nodeIp = nodes.get(i).getIp();
            if (!nodeIp.equals(NodeInfo.INSTANCE.getIp())) {
                int port = Integer.parseInt(nodes.get(i).getPort());
                blockRecordCount[i] = 0;
                for (int j = 0; j < memoryBlocks[i].length; j++) {
                    memoryBlocks[i][j] = new byte[dataSize * maxNoOfRecords];
                }
                currentMemoryBlockId[i] = 0;
                replicaDataSenders[i] = new ReplicaDataSender(nodeIp, port, dataStore.getHungryHippoFilePath(), memoryBlocks[i]);
                replicaDataSenders[i].start();
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
        for (int i = 0; i < replicaDataSenders.length; i++) {
            if (replicaDataSenders[i] != null) {
                replicaDataSenders[i].kill();
                replicaDataSenders[i].join();
                replicaDataSenders[i].publishRemainingReplicaData(blockRecordCount[i]*dataSize, currentMemoryBlockId[i]);
                replicaDataSenders[i].closeConnection();
                replicaDataSenders[i].clearReferences();
            }
        }
        //  waitForDataPublishersServerConnectRetryInterval();
        dataReaderHandlerCounter--;
        if (dataReaderHandlerCounter <= 0) {
            dataStore.sync();
            byteBuf.release();
            byteBuf = null;
            ctx.channel().close();
            dataReaderHandlerCounter = 0;
        }
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
                System.arraycopy(buf, 0, memoryBlocks[nodeId][currentMemoryBlockId[nodeId]], blockRecordCount[nodeId] * dataSize, dataSize);
                blockRecordCount[nodeId]++;
                if (blockRecordCount[nodeId] == maxNoOfRecords) {
                    switchBlock(nodeId);
                }
            }
            int storeId = nodeDataStoreIdCalculator.storeId(byteBuffer);
            dataStore.storeRow(storeId, buf);
        }

    }

    private void switchBlock(int nodeId){
        replicaDataSenders[nodeId].setMemoryBlockStatus(ReplicaDataSender.Status.ENABLE_BLOCK_READ, currentMemoryBlockId[nodeId]);
        currentMemoryBlockId[nodeId] = -1;
        while (currentMemoryBlockId[nodeId]==-1) {
            for (int j = 0; j < memoryBlocks[nodeId].length; j++) {
                if(replicaDataSenders[nodeId].getMemoryBlockStatus(j) == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE){
                    currentMemoryBlockId[nodeId] = j;
                    break;
                }
            }
        }
        blockRecordCount[nodeId] = 0;
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

    @Override
    public int hashCode() {
        return dataReaderHandlerId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ClientDataReadHandler) {
            return dataReaderHandlerId == ((ClientDataReadHandler) obj).dataReaderHandlerId;
        }
        return false;
    }

}
