package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;

/**
 * Created by debasishc on 1/9/15.
 */
public class DataReadHandler extends ChannelHandlerAdapter {
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
        byteBuf = ctx.alloc().buffer(dataDescription.getSize()*1000); // (1)
        //totalCount=0;
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
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
        ByteBuf msgB = (ByteBuf) msg;
        byteBuf.writeBytes(msgB); // (2)
        msgB.release();
       // System.out.println(byteBuf.readableBytes());
        while (byteBuf.readableBytes() >= dataDescription.getSize()) {
            byteBuf.readBytes(buf);
            dataStore.storeRow(byteBuffer, buf);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
