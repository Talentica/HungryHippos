package com.talentica.consumers.http;

/**
 * Created by debasishc on 15/6/15.
 */

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.*;

@ChannelHandler.Sharable

public class RequestDecoder extends SimpleChannelInboundHandler<HttpObject>  {


    public RequestDecoder() {
        // Do not autorelease HttpObject since
        // it is passed through
        super(false);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject httpObject)
            throws Exception {

        DecoderResult result = httpObject.getDecoderResult();
        if (!result.isSuccess()) {
            //throw new BadRequestException(result.cause());
        }

        if(httpObject instanceof HttpContent){
            ctx.fireChannelRead(((HttpContent) httpObject).content());
        }

    }
}