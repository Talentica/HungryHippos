package com.talentica.hungryHippos.node;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;

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

    public DataReadHandler(DataDescription dataDescription, DataStore dataStore) {
        this.dataDescription = dataDescription;
        this.buf = new byte[dataDescription.getSize()];
        byteBuffer = ByteBuffer.wrap(this.buf);
        this.dataStore = dataStore;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        byteBuf = ctx.alloc().buffer(dataDescription.getSize()*20); // (1)
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        while (byteBuf.readableBytes() >= dataDescription.getSize()) {
            byteBuf.readBytes(buf);
            dataStore.storeRow(byteBuffer, buf);
        }
        dataStore.sync();
        byteBuf.release(); // (1)
        byteBuf = null;
        ctx.channel().close();
        ctx.channel().parent().close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { 
        ByteBuf msgB = (ByteBuf) msg;
        byteBuf.writeBytes(msgB); // (2)
        msgB.release();
       //process the new data.
        while (byteBuf.readableBytes() >= dataDescription.getSize()) {
            byteBuf.readBytes(buf);
            dataStore.storeRow(byteBuffer, buf);
        }

        //take the remaining content to the beginning
        int remainingBytes = byteBuf.readableBytes();
        byteBuf.readBytes(buf, 0, remainingBytes);
        byteBuf.clear();
        byteBuf.writeBytes(buf,0,remainingBytes);
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
