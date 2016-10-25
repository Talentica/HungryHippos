package com.talentica.hungryHippos.node.datareceiver;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

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
  private int memoryArrayCapacity;
  private boolean flagToReadFileId;
  private int fileId;
  private RequestHandlingTool requestHandlingTool = null;
  private byte[] fileIdInBytes;
  private String senderIp;
  private boolean memoryLockAcquired;
  private int recoveryDataSize;
  private int receiverNodeId;

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    senderIp = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
    nodeIdClient = getNodeId(senderIp);
    LOGGER.info("Connected to {}", senderIp);
    byteBuf = ctx.alloc().buffer(2000);
    ReplicaDataSender replicaDataSender = ReplicaDataSender.INSTANCE;
    nodeIdToMemoryArraysMap = replicaDataSender.getNodeIdToMemoryArraysMap();
    nodeIdToMemoryArrayStoredDataLength =
        replicaDataSender.getNodeIdToMemoryArrayStoredDataLength();
    nodeIdToMemoryArrayStatusMap = replicaDataSender.getNodeIdToMemoryArrayStatusMap();
    memoryArrayCapacity = replicaDataSender.getMemoryArrayCapacity();
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
        if (requestHandlingTool.getReplicaNodesInfoDataSize() != 0) {
          processData();
          flagToReadFileId = true;
          if (byteBuf.readableBytes() >= FILE_ID_BYTE_SIZE) {
            switchFileId();
          } else {
            break;
          }
        } else {
          processOneDimesion();
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

  private void processOneDimesion() {
    byte[] dataForFileWrite = requestHandlingTool.getDataForFileWrite();
    byteBuf.readBytes(dataForFileWrite);
    requestHandlingTool.storeData();
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
    boolean writeDataFile = true;
    for (int i = 0; i < replicaNodesInfoDataSize; i++) {
      if (nextNodesInfo[i] != (byte) -1) {
        if (nextNodesInfo[i] != NodeInfo.INSTANCE.getIdentifier()) {
          receiverNodeId = nextNodesInfo[i];
          writeDataForNextNode(nextNodesInfo, dataForFileWrite, replicaNodesInfoDataSize, dataSize,
              i);
        } else {
          writeDataFile = false;
          if (nodeIdClient == NodeInfo.INSTANCE.getIdentifier()) {
            for (Integer nodeId : nodeIdToMemoryArraysMap.keySet()) {
              this.receiverNodeId = nodeId;
              writeEndOfDataSignal(nextNodesInfo, dataForFileWrite, replicaNodesInfoDataSize,
                  dataSize);
            }
          } else {
            int signalCountDown = EndOfDataTracker.INSTANCE.getCountDown(fileId, i);
            LOGGER.info("NodeIdClient {} Current count for FileId {} for dimension Index {} is :{}",
                nodeIdClient, fileId, i, signalCountDown);
            if (signalCountDown == 0) {
              if (i == replicaNodesInfoDataSize - 1) {
                String destinationPath = requestHandlingTool.getHhFilePath();
                ShardingApplicationContext context = requestHandlingTool.getContext();
                for (Integer nodeId : nodeIdToMemoryArraysMap.keySet()) {
                  RequestHandlingTool requestHandlingTool =
                      RequestHandlersCache.INSTANCE.get(nodeId, fileId);
                  requestHandlingTool.close();
                }
                EndOfDataTracker.INSTANCE.remove(fileId, destinationPath, context);
              } else {
                for (Integer nodeId : nodeIdToMemoryArraysMap.keySet()) {
                  this.receiverNodeId = nodeId;
                  writeEndOfDataSignal(nextNodesInfo, dataForFileWrite, replicaNodesInfoDataSize,
                      dataSize, i);
                }
              }
            }
          }
        }

        break;
      }

    }
    if (writeDataFile) {
      requestHandlingTool.storeData();
    }
  }

  private void writeEndOfDataSignal(byte[] nextNodesInfo, byte[] dataForFileWrite,
      int replicaNodesInfoDataSize, int dataSize) {
    LOGGER.info("Writing End of Data Signal for node :{} of nodeIdClient : {}", receiverNodeId,
        nodeIdClient);
    releaseAllMemoryArray();
    acquireLastMemoryArrayLock(dataSize);
    int currentIndex =
        nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[currentMemoryArrayId];
    byte[] currentMemoryArray = nodeIdToMemoryArraysMap.get(receiverNodeId)[currentMemoryArrayId];
    System.arraycopy(fileIdInBytes, 0, currentMemoryArray, currentIndex, FILE_ID_BYTE_SIZE);
    currentIndex += FILE_ID_BYTE_SIZE;
    nextNodesInfo[0] = (byte) receiverNodeId;
    System.arraycopy(nextNodesInfo, 0, currentMemoryArray, currentIndex, replicaNodesInfoDataSize);
    currentIndex += replicaNodesInfoDataSize;
    System.arraycopy(dataForFileWrite, 0, currentMemoryArray, currentIndex,
        dataForFileWrite.length);
    currentIndex += dataForFileWrite.length;
    releaseMemoryArrayForReading(currentIndex);
    LOGGER.info("Completed Writing End of Data Signal for node {} of nodeIdClient : {}",
        receiverNodeId, nodeIdClient);
  }

  private void writeEndOfDataSignal(byte[] nextNodesInfo, byte[] dataForFileWrite,
      int replicaNodesInfoDataSize, int dataSize, int i) {
    LOGGER.info("Writing End of Data Signal for node :{} of nodeIdClient : {}", receiverNodeId,
        nodeIdClient);
    releaseAllMemoryArray();
    acquireLastMemoryArrayLock(dataSize);
    int currentIndex =
        nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[currentMemoryArrayId];
    byte[] currentMemoryArray = nodeIdToMemoryArraysMap.get(receiverNodeId)[currentMemoryArrayId];
    System.arraycopy(fileIdInBytes, 0, currentMemoryArray, currentIndex, FILE_ID_BYTE_SIZE);
    currentIndex += FILE_ID_BYTE_SIZE;
    for (int j = 0; j <= i; j++) {
      currentMemoryArray[currentIndex + j] = (byte) -1;
    }
    currentIndex += i + 1;
    nextNodesInfo[i + 1] = (byte) receiverNodeId;
    System.arraycopy(nextNodesInfo, i + 1, currentMemoryArray, currentIndex,
        replicaNodesInfoDataSize - (i + 1));
    currentIndex += replicaNodesInfoDataSize - (i + 1);
    System.arraycopy(dataForFileWrite, 0, currentMemoryArray, currentIndex,
        dataForFileWrite.length);
    currentIndex += dataForFileWrite.length;
    releaseMemoryArrayForReading(currentIndex);
    LOGGER.info("Completed Writing End of Data Signal for node {} of nodeIdClient : {}",
        receiverNodeId, nodeIdClient);
  }

  private void writeDataForNextNode(byte[] nextNodesInfo, byte[] dataForFileWrite,
      int replicaNodesInfoDataSize, int dataSize, int i) {
    acquireMemoryArrayLock(dataSize);
    int currentIndex =
        nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[currentMemoryArrayId];
    byte[] currentMemoryArray = nodeIdToMemoryArraysMap.get(receiverNodeId)[currentMemoryArrayId];
    System.arraycopy(fileIdInBytes, 0, currentMemoryArray, currentIndex, FILE_ID_BYTE_SIZE);
    currentIndex += FILE_ID_BYTE_SIZE;
    for (int j = 0; j <= i; j++) {
      currentMemoryArray[currentIndex + j] = (byte) -1;
    }
    currentIndex += i + 1;
    System.arraycopy(nextNodesInfo, i + 1, currentMemoryArray, currentIndex,
        replicaNodesInfoDataSize - (i + 1));
    currentIndex += replicaNodesInfoDataSize - (i + 1);
    System.arraycopy(dataForFileWrite, 0, currentMemoryArray, currentIndex,
        dataForFileWrite.length);
    currentIndex += dataForFileWrite.length;
    releaseMemoryArrayForWriting(currentIndex);
  }

  private void acquireMemoryArrayLock(int dataSize) {
    ReplicaDataSender.Status[] statuses = nodeIdToMemoryArrayStatusMap.get(receiverNodeId);
    currentMemoryArrayId = -1;
    while (currentMemoryArrayId < 0) {
      for (int j = 0; j < statuses.length; j++) {
        if (statuses[j] == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
          synchronized (nodeIdToMemoryArraysMap.get(receiverNodeId)[j]) {
            if (statuses[j] == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
              if (nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[j]
                  + dataSize > memoryArrayCapacity) {
                statuses[j] = ReplicaDataSender.Status.ENABLE_BLOCK_READ;
                continue;
              }
              recoveryDataSize = nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[j];
              memoryLockAcquired = true;
              currentMemoryArrayId = j;
              statuses[j] = ReplicaDataSender.Status.WRITE_IN_PROGRESS;
              break;
            }
          }
        }
      }
    }
  }

  private void acquireLastMemoryArrayLock(int dataSize) {
    LOGGER.info("Acquiring lock on last memory block of node {} for nodeIdClient {}",
        receiverNodeId, nodeIdClient);
    ReplicaDataSender.Status[] statuses = nodeIdToMemoryArrayStatusMap.get(receiverNodeId);
    int lastMemoryArrayId = statuses.length - 1;
    currentMemoryArrayId = -1;
    while (currentMemoryArrayId < 0) {
      if (statuses[lastMemoryArrayId] == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
        synchronized (nodeIdToMemoryArraysMap.get(receiverNodeId)[lastMemoryArrayId]) {
          if (statuses[lastMemoryArrayId] == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
            if (nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[lastMemoryArrayId]
                + dataSize > memoryArrayCapacity) {
              statuses[lastMemoryArrayId] = ReplicaDataSender.Status.ENABLE_BLOCK_READ;
              continue;
            }
            recoveryDataSize =
                nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[lastMemoryArrayId];
            memoryLockAcquired = true;
            statuses[lastMemoryArrayId] = ReplicaDataSender.Status.WRITE_IN_PROGRESS;
            currentMemoryArrayId = lastMemoryArrayId;
            break;
          }
        }
      } else {
        LOGGER.info("Status of {}th memory array is {}", lastMemoryArrayId,
            statuses[lastMemoryArrayId].name());
      }
      if (statuses[lastMemoryArrayId] == ReplicaDataSender.Status.TRANSFER_IN_PROGRESS) {
        if (lastMemoryArrayId == 0) {
          lastMemoryArrayId = statuses.length;
        }
        lastMemoryArrayId = (lastMemoryArrayId - 1) % statuses.length;
        LOGGER.info("Status of {}th memory array is {}", lastMemoryArrayId,
            statuses[lastMemoryArrayId].name());
      }
    }
    LOGGER.info("Acquired lock on last memory block of node {} for nodeIdClient {}", receiverNodeId,
        nodeIdClient);
  }

  private void releaseAllMemoryArray() {
    ReplicaDataSender.Status[] statuses = nodeIdToMemoryArrayStatusMap.get(receiverNodeId);
    for (int j = 0; j < statuses.length; j++) {
      while (statuses[j] != ReplicaDataSender.Status.ENABLE_BLOCK_READ
          && statuses[j] != ReplicaDataSender.Status.TRANSFER_IN_PROGRESS) {
        synchronized (nodeIdToMemoryArraysMap.get(receiverNodeId)[j]) {
          if (statuses[j] == ReplicaDataSender.Status.ENABLE_BLOCK_WRITE) {
            if (nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[j] != 0) {
              statuses[j] = ReplicaDataSender.Status.ENABLE_BLOCK_READ;
            }
            break;
          }
        }
      }
    }
  }


  private void releaseMemoryArrayForReading(int currentIndex) {
    nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[currentMemoryArrayId] = currentIndex;
    nodeIdToMemoryArrayStatusMap.get(receiverNodeId)[currentMemoryArrayId] =
        ReplicaDataSender.Status.ENABLE_BLOCK_READ;
    memoryLockAcquired = false;
  }

  private void releaseMemoryArrayForWriting(int currentIndex) {
    nodeIdToMemoryArrayStoredDataLength.get(receiverNodeId)[currentMemoryArrayId] = currentIndex;
    nodeIdToMemoryArrayStatusMap.get(receiverNodeId)[currentMemoryArrayId] =
        ReplicaDataSender.Status.ENABLE_BLOCK_WRITE;
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
    if (requestHandlingTool.getReplicaNodesInfoDataSize() == 0) {
      EndOfDataTracker.INSTANCE.remove(fileId, requestHandlingTool.getHhFilePath(),
          requestHandlingTool.getContext());
    }
    if (nodeIdClient != NodeInfo.INSTANCE.getIdentifier()) {
      RequestHandlersCache.INSTANCE.removeAllRequestHandlingTool(nodeIdClient);
      RequestHandlingShardingInfoCache.INSTANCE.handleRemoveAll(nodeIdClient);
    } else {
      requestHandlingTool.close();
    }

    LOGGER.info("Disconnected from {}", senderIp);

  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOGGER.error("Exception from Node {} : {}", nodeIdClient, cause.getMessage());
    ctx.fireExceptionCaught(cause);
    cause.printStackTrace();
    LOGGER.error("Disconnecting from {}", senderIp);
    if (memoryLockAcquired) {
      if (recoveryDataSize > 0) {
        releaseMemoryArrayForReading(recoveryDataSize);
      } else {
        releaseMemoryArrayForWriting(recoveryDataSize);
      }
    }
    ctx.channel().close();
  }
}
