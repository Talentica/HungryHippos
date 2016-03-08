package com.talentica.hungryHippos.node;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.server.ServerUtils;

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
	private ByteBuffer byteBuffer;
	private DataStore dataStore;
	private ByteBuf byteBuf;
	private NodeDataStoreIdCalculator nodeDataStoreIdCalculator;

	private static int dataReaderHandlerCounter = 0;
	private int dataReaderHandlerId = -1;

	public DataReadHandler(DataDescription dataDescription, DataStore dataStore) throws IOException {
		this.dataDescription = dataDescription;
		this.buf = new byte[dataDescription.getSize()];
		byteBuffer = ByteBuffer.wrap(this.buf);
		this.dataStore = dataStore;
		dataReaderHandlerId = ++dataReaderHandlerCounter;
		nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(NodeUtil.getKeyToValueToBucketMap(),
				NodeUtil.getBucketToNodeNumberMap(), NodeUtil.getNodeId(), dataDescription);
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) {
		byteBuf = ctx.alloc().buffer(dataDescription.getSize() * 20); // (1)
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws InterruptedException {
		while (byteBuf.readableBytes() >= dataDescription.getSize()) {
			byteBuf.readBytes(buf);
			int storeId = nodeDataStoreIdCalculator.storeId(byteBuffer);
			dataStore.storeRow(storeId, byteBuffer, buf);
		}
		waitForDataPublishersServerConnectRetryInterval();
		dataReaderHandlerCounter--;
		if (dataReaderHandlerCounter <= 0) {
			dataStore.sync();
			byteBuf.release();
			byteBuf = null;
			ctx.channel().close();
			ctx.channel().parent().close();
			dataReaderHandlerCounter = 0;
		}
	}

	private void waitForDataPublishersServerConnectRetryInterval() throws InterruptedException {
		Thread.sleep(ServerUtils.SERVER_CONNECT_RETRY_INTERVAL * 2);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		ByteBuf msgB = (ByteBuf) msg;
		byteBuf.writeBytes(msgB); // (2)
		msgB.release();
		// process the new data.
		while (byteBuf.readableBytes() >= dataDescription.getSize()) {
			byteBuf.readBytes(buf);
			int storeId = nodeDataStoreIdCalculator.storeId(byteBuffer);
			dataStore.storeRow(storeId, byteBuffer, buf);
		}

		// take the remaining content to the beginning
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
