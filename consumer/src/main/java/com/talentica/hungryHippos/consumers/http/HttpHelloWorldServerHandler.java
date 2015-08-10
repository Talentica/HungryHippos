/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.talentica.hungryHippos.consumers.http;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.google.protobuf.InvalidProtocolBufferException;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.talentica.hungryHippos.msg.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaders.Values;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


@ChannelHandler.Sharable
public class HttpHelloWorldServerHandler extends
        SimpleChannelInboundHandler<ByteBuf> {
    private static final byte[] CONTENT = { 'H', 'e', 'l', 'l', 'o', ' ', 'W',
            'o', 'r', 'l', 'd' };

    private static byte[] buf = new byte[1024];

    private static Message.MessageDemo messgeDemo = Message.MessageDemo.getDefaultInstance();


    private static final RingBuffer<LongEvent> ringBuffer = HttpHelloWorldServerHandler.createDisruptor();

    private static RingBuffer<LongEvent> createDisruptor()
    {
        Executor executor = Executors.newCachedThreadPool();
        LongEventFactory factory = new LongEventFactory();
        Disruptor<LongEvent> disruptor = new Disruptor<>(factory, 4096, executor, ProducerType.SINGLE, new BlockingWaitStrategy() );
        disruptor.handleEventsWith(new LongEventHandler());
        disruptor.start();
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
        return ringBuffer;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf req)
            throws Exception {

        Thread.currentThread().setName("Ramprasad");
//
//        long sequence = ringBuffer.next();
//        LongEvent event = ringBuffer.get(sequence);
        ringBuffer.publishEvent(new EventTranslatorOneArg<LongEvent, Object>() {
            @Override
            public void translateTo(LongEvent event, long sequence, Object arg0) {
                try {
                    int length = ((ByteBuf)arg0).readableBytes();
                    ((ByteBuf)arg0).readBytes(buf, 0, length);
                    event.set(messgeDemo.getParserForType()
                            .parseFrom(buf, 0, length).getName());
                  //  System.out.println(event.get());
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }

            }
        }, req);


        //ringBuffer.publish(sequence);
        //System.out.println(req.getClass());
        //System.out.println("Event: " + req.getName());


//		if (HttpHeaders.is100ContinueExpected(req)) {
//			ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
//		}
        boolean keepAlive = true; //HttpHeaders.isKeepAlive(req);
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
                Unpooled.wrappedBuffer(CONTENT));
        response.headers().set(CONTENT_TYPE, "text/plain");
        response.headers().set(CONTENT_LENGTH,
                response.content().readableBytes());

        if (!keepAlive) {
            ctx.write(response).addListener(ChannelFutureListener.CLOSE);
        } else {
            response.headers().set(CONNECTION, Values.KEEP_ALIVE);
            ctx.write(response);
        }

    }
}