package com.talentica.hungryHippos.node;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.utility.ShuffleArrayUtil;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * Created by debasishc on 1/9/15.
 */
public class DataReadHandler extends ChannelHandlerAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataReadHandler.class);
  private DataDescription dataDescription;
  private byte[] buf;
  private byte[] previousHandlerUnprocessedData;
  private ByteBuffer byteBuffer;
  private DataStore dataStore;
  private ByteBuf byteBuf;
  private NodeDataStoreIdCalculator nodeDataStoreIdCalculator;

  private static int dataReaderHandlerCounter = 0;
  private int dataReaderHandlerId = -1;
  private int recordSize;
  private int replicaNodesInfoDataSize;
  private Map<String,List<byte[]>> nodeReplicaDataMap;
  private List<Node> nodeList;
  private long batchRecCount = 0;

  public DataReadHandler(DataDescription dataDescription, DataStore dataStore,
      byte[] remainingBufferData, NodeUtil nodeUtil, ShardingApplicationContext context)
      throws IOException {
    this.previousHandlerUnprocessedData = remainingBufferData;
    this.dataDescription = dataDescription;
    this.buf = new byte[dataDescription.getSize()];
    byteBuffer = ByteBuffer.wrap(this.buf);
    this.dataStore = dataStore;
    dataReaderHandlerId = ++dataReaderHandlerCounter;
    int shardingDimensions= context.getShardingDimensions().length;
    replicaNodesInfoDataSize = shardingDimensions-1;
    recordSize = replicaNodesInfoDataSize +dataDescription.getSize();
    nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(nodeUtil.getKeyToValueToBucketMap(),
        nodeUtil.getBucketToNodeNumberMap(), NodeInfo.INSTANCE.getIdentifier(), dataDescription,context);
   nodeList = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
    nodeReplicaDataMap = new HashMap<>();
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws IOException, InterruptedException {
    byteBuf = ctx.alloc().buffer(recordSize * 20);
    byteBuf.writeBytes(previousHandlerUnprocessedData);
    processData();
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws InterruptedException, IOException {
    writeDataInStore();
    publishReplicaData();
  //  waitForDataPublishersServerConnectRetryInterval();
    dataReaderHandlerCounter--;
    if (dataReaderHandlerCounter <= 0) {
      dataStore.sync();
      byteBuf.release();
      byteBuf = null;
      ctx.channel().close();
      dataReaderHandlerCounter = 0;
    }
  }

  private void waitForDataPublishersServerConnectRetryInterval() throws InterruptedException {
    // TODO NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE value should be discussed.
    int NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE = Integer.valueOf(5);
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
    checkBatchCountAndPublishReplicaData();
  }

  /**
   * Writes the data in DataStore
   */
  private void writeDataInStore() {

    while (byteBuf.readableBytes() >= recordSize) {
      byte[] nextNodesInfo = new byte[replicaNodesInfoDataSize];
      byteBuf.readBytes(nextNodesInfo);
      byteBuf.readBytes(buf);
      for (int i = 0; i < replicaNodesInfoDataSize; i++) {
        if(nextNodesInfo[i]!=(byte)-1){
          byte[] dataForNextNode = new byte[recordSize];
          for (int j = 0; j <= i; j++) {
            dataForNextNode[j] = (byte) -1;
          }
          System.arraycopy(nextNodesInfo,i+1,dataForNextNode,i+1, replicaNodesInfoDataSize);
          System.arraycopy(buf,0,dataForNextNode, replicaNodesInfoDataSize,buf.length);
          List<byte[]> dataList = nodeReplicaDataMap.get(nextNodesInfo[i]+"");
          if(dataList==null){
            dataList = new ArrayList<>();
            nodeReplicaDataMap.put(nextNodesInfo[i]+"",dataList);
          }
          dataList.add(dataForNextNode);
          batchRecCount++;
          break;
        }
      }

      int storeId = nodeDataStoreIdCalculator.storeId(byteBuffer);
      dataStore.storeRow(storeId, byteBuffer, buf);
    }


  }

  /**
   * Checks the size of data in the batch and sends the Replica Data if it touches trigger point
   * @throws IOException
   * @throws InterruptedException
     */
  public void checkBatchCountAndPublishReplicaData() throws IOException, InterruptedException {
    if(batchRecCount>=100000){
      publishReplicaData();
    }
  }

  /**
   * Publishes the replica data to the all the other nodes
   * @throws IOException
   * @throws InterruptedException
     */
  public void publishReplicaData() throws IOException, InterruptedException {
    Object[] nodeArray =  nodeList.toArray();
    ShuffleArrayUtil.shuffleArray(nodeArray);
    for (int i = 0; i < nodeArray.length; i++) {
      Node node = (Node) nodeArray[i];
      publishData(node.getIp(),node.getPort(), nodeReplicaDataMap.get(node.getIdentifier()+""));
    }
    batchRecCount=0;
  }

  /**
   * Publishes the dataList to a particular node
   * @param nodeIp
   * @param port
   * @param dataList
   * @throws IOException
   * @throws InterruptedException
     */
  public void publishData(String nodeIp, String port, List<byte[]> dataList) throws IOException, InterruptedException {
    if (dataList==null || dataList.size() == 0) {
      return;
    }
    LOGGER.info("Pushing data of size {} to node : {}",dataList.size(),nodeIp);
    byte[] destinationPathInBytes = dataStore.getHungryHippoFilePath().getBytes(Charset.defaultCharset());
    int destinationPathLength = destinationPathInBytes.length;
    int noOfAttemptsToConnectToNode = 50;
    Socket clientSocket = ServerUtils.connectToServer(nodeIp + ":" + port, noOfAttemptsToConnectToNode);
    DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
    BufferedOutputStream bos = new BufferedOutputStream(clientSocket.getOutputStream(), 8388608);
    dos.writeInt(destinationPathLength);
    dos.flush();
    bos.write(destinationPathInBytes);
    for (int i = 0; i < dataList.size(); i++) {
      bos.write(dataList.get(i));
    }
    bos.flush();
    dataList.clear();
    bos.close();
    clientSocket.close();
    LOGGER.info("Pushing data to node {} completed",nodeIp);
  }

  /**
   * reInitializes byteBuf
   */
  private void reInitializeByteBuf() {
    int remainingBytes = byteBuf.readableBytes();
    byteBuf.readBytes(buf, 0, remainingBytes);
    byteBuf.clear();
    byteBuf.writeBytes(buf, 0, remainingBytes);
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
    if (obj instanceof DataReadHandler) {
      return dataReaderHandlerId == ((DataReadHandler) obj).dataReaderHandlerId;
    }
    return false;
  }

}
