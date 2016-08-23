package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.IOException;

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
      String dataAbsolutePath = FileSystemContext.getRootDirectory() + hhFilePath;
      String shardingTableFolderPath = dataAbsolutePath+ File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
      updateFilesIfRequired(shardingTableFolderPath);
      ShardingApplicationContext context = new ShardingApplicationContext(shardingTableFolderPath);
      FieldTypeArrayDataDescription dataDescription =
          context.getConfiguredDataDescription();
      dataDescription.setKeyOrder(context.getShardingDimensions());
      NodeUtil nodeUtil = new NodeUtil(hhFilePath);
      DataStore dataStore = new FileDataStore(nodeUtil.getKeyToValueToBucketMap().size(),
          dataDescription, hhFilePath, nodeId,context);
      ctx.pipeline().remove(DataReceiver.REQUEST_DETAILS_HANDLER);
      ctx.pipeline().addLast(DataReceiver.DATA_HANDLER,
          new DataReadHandler(dataDescription, dataStore, remainingBufferData, nodeUtil,context));
    }
  }

  /**
   * Updates the sharding files if required
   * @param shardingTableFolderPath
   * @throws IOException
     */
  private void updateFilesIfRequired(String shardingTableFolderPath) throws IOException {
    String shardingTableZipPath = shardingTableFolderPath+".tar.gz";
    File shardingTableFolder = new File(shardingTableFolderPath);
    File shardingTableZip = new File(shardingTableZipPath);
    if(shardingTableFolder.exists()){
      if(shardingTableFolder.lastModified()<shardingTableZip.lastModified()){
        FileSystemUtils.deleteFilesRecursively(shardingTableFolder);
        TarAndGzip.untarTGzFile(shardingTableZipPath);
      }
    }else{
      TarAndGzip.untarTGzFile(shardingTableZipPath);
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
