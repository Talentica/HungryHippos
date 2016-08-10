package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by rajkishoreh on 22/7/16.
 */
public class RequestDetailsHandler extends ChannelHandlerAdapter {

  private ByteBuf byteBuf;
  private String nodeId;

  private boolean isFilePathLengthRead = false;
  int filePathLength = 0;

  public RequestDetailsHandler(String nodeId) {
    super();
    this.nodeId = nodeId;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    byteBuf = ctx.alloc().buffer(4);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    byteBuf.release();
    byteBuf = null;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

    ByteBuf m = (ByteBuf) msg;
    byteBuf.writeBytes(m);
    m.release();

    if (byteBuf.readableBytes() >= 4 && !isFilePathLengthRead) {
      filePathLength = byteBuf.readInt();
      isFilePathLengthRead = true;
    }
    if (byteBuf.readableBytes() >= filePathLength && isFilePathLengthRead) {

      String hhFilePath = readHHFilePath();
      byte[] remainingBufferData = new byte[byteBuf.readableBytes()];
      byteBuf.readBytes(remainingBufferData);

      String nodeId = NodeInfo.INSTANCE.getId();
      DataDescription dataDescription =
          ShardingApplicationContext.getConfiguredDataDescription();
      // TODO Get sharding table for the particular file from zookeeper instead of using common
      // config
      NodeUtil nodeUtil = new NodeUtil(hhFilePath);
      DataStore dataStore = new FileDataStore(nodeUtil.getKeyToValueToBucketMap().size(),
          dataDescription, hhFilePath, nodeId);

      
      ctx.pipeline().remove(DataReceiver.REQUEST_DETAILS_HANDLER);
      ctx.pipeline().addLast(DataReceiver.DATA_HANDLER,
          new DataReadHandler(dataDescription, dataStore, remainingBufferData, nodeUtil));
    }
  }

  /**
   * Returns HHFilePath using the filePathLength
   * 
   * @return
   */
  private String readHHFilePath() {
    byte[] hhFilePathInBytes = new byte[filePathLength];
    byteBuf.readBytes(hhFilePathInBytes);
    return new String(hhFilePathInBytes);
  }

}
