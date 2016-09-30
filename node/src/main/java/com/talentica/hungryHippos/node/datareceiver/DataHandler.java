package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Created by rajkishoreh on 22/9/16.
 */
public class DataHandler extends ChannelHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataHandler.class);
    private ByteBuf byteBuf;
    private RequestHandlersCache requestHandlersCache;
    private int nodeIdClient;
    private Map<Integer, byte[][]> nodeIdToMemoryArraysMap;
    private int currentMemoryArrayId;
    private Map<Integer, int[]> nodeIdToMemoryArrayStoredDataLength;
    private Map<Integer, ReplicaDataSender.Status[]> nodeIdToMemoryArrayStatusMap;
    public static final int FILE_ID_BYTE_SIZE = 4;
    private int memoryBlockCapacity;
    private boolean flagToReadFileId;
    private int fileId;
    private RequestHandlingTool requestHandlingTool = null;
    private byte[] fileIdInBytes;
    private String senderIp;
    private boolean memoryLockAcquired;
    private int recoveryDataSize;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        senderIp = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        nodeIdClient = getNodeId(senderIp);
        LOGGER.info("Connected to {}", senderIp);
        byteBuf = ctx.alloc().buffer(2000);
        ReplicaDataSender replicaDataSender = ReplicaDataSender.INSTANCE;
        nodeIdToMemoryArraysMap = replicaDataSender.getNodeIdToMemoryArraysMap();
        nodeIdToMemoryArrayStoredDataLength = replicaDataSender.getNodeIdToMemoryArrayStoredDataLength();
        nodeIdToMemoryArrayStatusMap = replicaDataSender.getNodeIdToMemoryArrayStatusMap();
        memoryBlockCapacity = replicaDataSender.getMemoryBlockCapacity();
        requestHandlersCache = RequestHandlersCache.INSTANCE;
        flagToReadFileId = true;
        memoryLockAcquired = false;
        LOGGER.info("Handler successfully added for {}", senderIp);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            ByteBuf msgB = (ByteBuf) msg;
            byteBuf.writeBytes(msgB);
            msgB.release();
            while (byteBuf.readableBytes() >= FILE_ID_BYTE_SIZE && flagToReadFileId) {
                switchFileId();
            }
            while (byteBuf.readableBytes() >= requestHandlingTool.getRecordSize()) {
                processData();
                flagToReadFileId = true;
                if (byteBuf.readableBytes() >= FILE_ID_BYTE_SIZE) {
                    switchFileId();
                } else {
                    break;
                }
            }
            reInitializeByteBuf();
        } catch (Exception e) {
            reInitializeByteBuf();
            e.printStackTrace();
            LOGGER.error("Exception from Node {} : {}", nodeIdClient, e.getMessage());
            LOGGER.error("Processing failed for Node {} : FileId {} :", nodeIdClient, fileId);
            throw new RuntimeException(e);
        }
    }

    private void switchFileId() {
        fileId = byteBuf.readInt();
        requestHandlingTool = requestHandlersCache.get(nodeIdClient, fileId);
        flagToReadFileId = false;
    }

    /**
     * reInitializes byteBuf
     */
    private void reInitializeByteBuf() {
        int remainingBytes = byteBuf.readableBytes();
        byte[] bufferForReset = new byte[remainingBytes];
        byteBuf.readBytes(bufferForReset, 0, remainingBytes);
        byteBuf.clear();
        byteBuf.writeBytes(bufferForReset, 0, remainingBytes);
    }


    public void processData() {
        byte[] nextNodesInfo = requestHandlingTool.getNextNodesInfo();
        byte[] dataForFileWrite = requestHandlingTool.getDataForFileWrite();
        byteBuf.readBytes(nextNodesInfo);
        byteBuf.readBytes(dataForFileWrite);
        int replicaNodesInfoDataSize = requestHandlingTool.getReplicaNodesInfoDataSize();
        fileIdInBytes = requestHandlingTool.getFileIdInBytes();
        int dataSize = requestHandlingTool.getRecordSize() + FILE_ID_BYTE_SIZE;
        if (nextNodesInfo[0] == (byte) nodeIdClient) {
            LOGGER.info("NodeId :{} Closing files",nodeIdClient);
            if (nodeIdClient == NodeInfo.INSTANCE.getIdentifier()) {
                for (Integer nodeId : nodeIdToMemoryArraysMap.keySet()) {
                    writeEndOfDataSignal(nextNodesInfo, dataForFileWrite, replicaNodesInfoDataSize, dataSize, nodeId);
                }
            }
            requestHandlingTool.close();
        } else {
            requestHandlingTool.storeData();
            for (int i = 0; i < replicaNodesInfoDataSize; i++) {
                int nodeId = (int) nextNodesInfo[i];
                if (nextNodesInfo[i] != (byte) -1) {
                    writeDataForNextNode(nextNodesInfo, dataForFileWrite, replicaNodesInfoDataSize, dataSize, i, nodeId);
                    break;
                }
            }
        }

    }

    private void writeEndOfDataSignal(byte[] nextNodesInfo, byte[] dataForFileWrite, int replicaNodesInfoDataSize, int dataSize, Integer nodeId) {
        LOGGER.info("Writing End of Data Signal for node :{}",nodeId);
        releaseAllMemoryArray(nodeId);
        acquireMemoryArrayLock(nodeId, dataSize);
        int currentIndex = nodeIdToMemoryArrayStoredDataLength.get(nodeId)[currentMemoryArrayId];
        byte[] currentMemoryArray = nodeIdToMemoryArraysMap.get(nodeId)[currentMemoryArrayId];
        System.arraycopy(fileIdInBytes, 0, currentMemoryArray, currentIndex, FILE_ID_BYTE_SIZE);
        currentIndex += FILE_ID_BYTE_SIZE;
        nextNodesInfo[0] = (byte) NodeInfo.INSTANCE.getIdentifier();
        System.arraycopy(nextNodesInfo, 0, currentMemoryArray, currentIndex, replicaNodesInfoDataSize);
        currentIndex += replicaNodesInfoDataSize;
        System.arraycopy(dataForFileWrite, 0, currentMemoryArray, currentIndex, dataForFileWrite.length);
        currentIndex += dataForFileWrite.length;
        releaseMemoryArrayForReading(nodeId, currentIndex);
        LOGGER.info("Completed Writing End of Data Signal for node {}",nodeId);
    }

    private void writeDataForNextNode(byte[] nextNodesInfo, byte[] dataForFileWrite, int replicaNodesInfoDataSize, int dataSize, int i, int nodeId) {
        acquireMemoryArrayLock(nodeId, dataSize);
        int currentIndex = nodeIdToMemoryArrayStoredDataLength.get(nodeId)[currentMemoryArrayId];
        byte[] currentMemoryArray = nodeIdToMemoryArraysMap.get(nodeId)[currentMemoryArrayId];
        System.arraycopy(fileIdInBytes, 0, currentMemoryArray, currentIndex, FILE_ID_BYTE_SIZE);
        currentIndex += FILE_ID_BYTE_SIZE;
        for (int j = 0; j <= i; j++) {
            currentMemoryArray[currentIndex + j] = (byte) -1;
        }
        currentIndex += i + 1;
        System.arraycopy(nextNodesInfo, i + 1, currentMemoryArray, currentIndex, replicaNodesInfoDataSize - (i + 1));
        currentIndex += replicaNodesInfoDataSize - (i + 1);
        System.arraycopy(dataForFileWrite, 0, currentMemoryArray, currentIndex, dataForFileWrite.length);
        releaseMemoryArrayForWriting(nodeId, currentIndex + dataForFileWrite.length);
    }

    private void acquireMemoryArrayLock(int nodeId, int dataSize) {
        currentMemoryArrayId = -1;
        while (currentMemoryArrayId < 0) {
            ReplicaDataSender.Status[] statuses = nodeIdToMemoryArrayStatusMap.get(nodeId);
            for (int j = 0; j < statuses.length; j++) {
                if (statuses[j] == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
                    synchronized (statuses[j]) {
                        if (statuses[j] == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
                            if (nodeIdToMemoryArrayStoredDataLength.get(nodeId)[j] + dataSize > memoryBlockCapacity) {
                                statuses[j] = ReplicaDataSender.Status.ENABLE_BLOCK_READ;
                                continue;
                            }
                            currentMemoryArrayId = j;
                            statuses[j] = ReplicaDataSender.Status.WRITE_IN_PROGRESS;
                            memoryLockAcquired = true;
                            recoveryDataSize = nodeIdToMemoryArrayStoredDataLength.get(nodeId)[j];
                            break;
                        }
                    }
                }
            }
        }
    }

    private void releaseAllMemoryArray(int nodeId) {
        ReplicaDataSender.Status[] statuses = nodeIdToMemoryArrayStatusMap.get(nodeId);
        for (int j = 0; j < statuses.length; j++) {
            while (statuses[j] != ReplicaDataSender.Status.ENABLE_BLOCK_READ) {
                synchronized (statuses[j]) {
                    if(nodeIdToMemoryArrayStoredDataLength.get(nodeId)[j]==0 && statuses[j] != ReplicaDataSender.Status.WRITE_IN_PROGRESS){
                        break;
                    }
                    if (statuses[j] == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
                        statuses[j] = ReplicaDataSender.Status.ENABLE_BLOCK_READ;
                    }
                }
            }
        }
        LOGGER.info("Released all Memory Array for node {}",nodeId);
    }


    private void releaseMemoryArrayForReading(Integer nodeId, int currentIndex) {
        nodeIdToMemoryArrayStoredDataLength.get(nodeId)[currentMemoryArrayId] = currentIndex;
        nodeIdToMemoryArrayStatusMap.get(nodeId)[currentMemoryArrayId] = ReplicaDataSender.Status.ENABLE_BLOCK_READ;
        memoryLockAcquired = false;
    }

    private void releaseMemoryArrayForWriting(int nodeId, int currentIndex) {
        nodeIdToMemoryArrayStoredDataLength.get(nodeId)[currentMemoryArrayId] = currentIndex;
        nodeIdToMemoryArrayStatusMap.get(nodeId)[currentMemoryArrayId] = ReplicaDataSender.Status.ENABLE_BLOCK_WRITE;
        memoryLockAcquired = false;
    }

    private int getNodeId(String ip) {
        ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
        List<Node> nodes = clusterConfig.getNode();
        for (Node node : nodes) {
            if (node.getIp().equals(ip)) {
                return node.getIdentifier();
            }
        }
        return NodeInfo.INSTANCE.getIdentifier();
    }


    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("Disconnected from {}", senderIp);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error("Exception from Node {} : {}", nodeIdClient, cause.getMessage());
        ctx.fireExceptionCaught(cause);
        cause.printStackTrace();
        LOGGER.error("Disconnecting from {}", senderIp);
        ctx.channel().close();
        if (memoryLockAcquired) {
            releaseMemoryArrayForReading(nodeIdClient, recoveryDataSize);
        }
        if (nodeIdClient != NodeInfo.INSTANCE.getIdentifier()) {
            RequestHandlersCache.INSTANCE.removeAllRequestHandlingTool(nodeIdClient);
            RequestHandlingShardingInfoCache.INSTANCE.handleRemoveAll(nodeIdClient);
        } else {
            requestHandlingTool.close();
        }
    }
}
