package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private Map<Integer, Integer> nodeIdToCurrentMemoryArrayIndexMap;
    private Map<Integer, Integer> nodeIdToCurrentMemoryArrayLastByteIndexMap;
    private Map<Integer, int[]> nodeIdToMemoryArrayLastByteIndex;
    private Map<Integer, BossReplicaDataSender.Status[]> nodeIdToMemoryArrayStatusMap;
    public static final int INT_BYTE_SIZE = 4;
    private int memoryBlockCapacity;
    private boolean flagToReadFileId = true;
    private int fileId;
    private RequestHandlingTool requestHandlingTool = null;
    private byte[] fileIdInBytes;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        String senderIp = ctx.channel().remoteAddress().toString();
        nodeIdClient = getNodeId(senderIp);
        LOGGER.info("Connected to {}",senderIp);
        byteBuf = ctx.alloc().buffer(2000);
        BossReplicaDataSender bossReplicaDataSender = BossReplicaDataSender.INSTANCE;
        nodeIdToCurrentMemoryArrayLastByteIndexMap = bossReplicaDataSender.getNodeIdToCurrentMemoryArrayLastByteIndexMap();
        nodeIdToCurrentMemoryArrayIndexMap = bossReplicaDataSender.getNodeIdToCurrentMemoryArrayIndexMap();
        nodeIdToMemoryArraysMap = bossReplicaDataSender.getNodeIdToMemoryArraysMap();
        nodeIdToMemoryArrayLastByteIndex = bossReplicaDataSender.getNodeIdToMemoryArrayLastByteIndex();
        nodeIdToMemoryArrayStatusMap = bossReplicaDataSender.getNodeIdToMemoryArrayStatusMap();
        memoryBlockCapacity = bossReplicaDataSender.getMemoryBlockCapacity();
        requestHandlersCache = RequestHandlersCache.INSTANCE;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgB = (ByteBuf) msg;
        byteBuf.writeBytes(msgB);
        msgB.release();
        while (byteBuf.readableBytes() >= INT_BYTE_SIZE && flagToReadFileId) {
            switchFileId();
        }
        while (byteBuf.readableBytes() >= requestHandlingTool.getRecordSize()) {
            processData();
            flagToReadFileId = true;
            if (byteBuf.readableBytes() >= INT_BYTE_SIZE) {
                switchFileId();
            } else {
                break;
            }
        }
        reInitializeByteBuf();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("Disconnected from {}",ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.info(cause.toString());
        ctx.fireExceptionCaught(cause);
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
        int dataSize = requestHandlingTool.getRecordSize() + INT_BYTE_SIZE;
        if(nextNodesInfo[0]==(byte) nodeIdClient){
            requestHandlingTool.closeDataStore();
            for(Integer nodeId:nodeIdToMemoryArraysMap.keySet()){
                int currentIndex = nodeIdToCurrentMemoryArrayLastByteIndexMap.get(nodeId);
                if ((currentIndex + dataSize) > memoryBlockCapacity) {
                    switchMemoryArray(nodeId, currentIndex);
                    currentIndex = 0;
                }
                byte[] currentMemoryArray = nodeIdToMemoryArraysMap.get(nodeId)[nodeIdToCurrentMemoryArrayIndexMap.get(nodeId)];
                System.arraycopy(fileIdInBytes, 0, currentMemoryArray, currentIndex, INT_BYTE_SIZE);
                nextNodesInfo[0] =(byte) NodeInfo.INSTANCE.getIdentifier();
                System.arraycopy(nextNodesInfo, 0, currentMemoryArray, currentIndex + INT_BYTE_SIZE, replicaNodesInfoDataSize);
                System.arraycopy(dataForFileWrite, 0, currentMemoryArray, currentIndex + replicaNodesInfoDataSize + INT_BYTE_SIZE, dataForFileWrite.length);
                nodeIdToCurrentMemoryArrayLastByteIndexMap.put(nodeId, currentIndex + dataSize);
            }
        }else{
            requestHandlingTool.storeData();
            for (int i = 0; i < replicaNodesInfoDataSize; i++) {
                int nodeId = (int) nextNodesInfo[i];
                if (nextNodesInfo[i] != (byte) -1) {
                    int currentIndex = nodeIdToCurrentMemoryArrayLastByteIndexMap.get(nodeId);
                    if ((currentIndex + dataSize) > memoryBlockCapacity) {
                        switchMemoryArray(nodeId, currentIndex);
                        currentIndex = 0;
                    }
                    byte[] currentMemoryArray = nodeIdToMemoryArraysMap.get(nodeId)[nodeIdToCurrentMemoryArrayIndexMap.get(nodeId)];
                    System.arraycopy(fileIdInBytes, 0, currentMemoryArray, currentIndex, INT_BYTE_SIZE);
                    for (int j = 0; j <= i; j++) {
                        currentMemoryArray[currentIndex + j] = (byte) -1;
                    }
                    System.arraycopy(nextNodesInfo, i + 1, currentMemoryArray, currentIndex + i + 1 + INT_BYTE_SIZE, replicaNodesInfoDataSize - (i + 1));
                    System.arraycopy(dataForFileWrite, 0, currentMemoryArray, currentIndex + replicaNodesInfoDataSize + INT_BYTE_SIZE, dataForFileWrite.length);
                    nodeIdToCurrentMemoryArrayLastByteIndexMap.put(nodeId, currentIndex + dataSize);
                    break;
                }
            }
        }

    }


    private void switchMemoryArray(int nodeId, int currentIndex) {
        int currentMemoryArray = nodeIdToCurrentMemoryArrayIndexMap.get(nodeId);
        nodeIdToMemoryArrayLastByteIndex.get(nodeId)[currentMemoryArray] = currentIndex;
        nodeIdToMemoryArrayStatusMap.get(nodeId)[currentMemoryArray] = BossReplicaDataSender.Status.ENABLE_BLOCK_READ;
        nodeIdToCurrentMemoryArrayIndexMap.put(nodeId, null);
        while (nodeIdToCurrentMemoryArrayIndexMap.get(nodeId) == null) {
            for (int j = 0; j < nodeIdToMemoryArraysMap.get(nodeId).length; j++) {
                if (nodeIdToMemoryArrayStatusMap.get(nodeId)[j] == BossReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
                    nodeIdToCurrentMemoryArrayIndexMap.put(nodeId, j);
                    break;
                }
            }
        }
        nodeIdToCurrentMemoryArrayLastByteIndexMap.put(nodeId, 0);
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
}
