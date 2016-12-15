package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.node.DataReceiver;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Created by rajkishoreh on 21/11/16.
 */
public class FileIdHandler extends ChannelHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NewDataHandler.class);
    private ByteBuf byteBuf;
    public static final int FILE_ID_BYTE_SIZE = 4;


    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        String senderIp = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        LOGGER.info("Connected to {}", senderIp);
        byteBuf = ctx.alloc().buffer(FILE_ID_BYTE_SIZE);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgB = (ByteBuf) msg;
        byteBuf.writeBytes(msgB);
        msgB.release();
        if (byteBuf.readableBytes() >= FILE_ID_BYTE_SIZE) {
            int fileId = byteBuf.readInt();
            byte[] remainingBufferData = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(remainingBufferData);
            ctx.pipeline().remove(DataReceiver.FILE_ID_HANDLER);
            ctx.pipeline().addLast(DataReceiver.DATA_HANDLER, new NewDataHandler(fileId, remainingBufferData));
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.fireExceptionCaught(cause);
        cause.printStackTrace();
        LOGGER.error(cause.toString());
    }

}
