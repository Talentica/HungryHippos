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

import com.talentica.hungryHippos.msg.Message;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.ssl.SslContext;

public class HttpHelloWorldServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private static final HttpHelloWorldServerHandler handler = new HttpHelloWorldServerHandler();
    private static final RequestDecoder requestDecoder = new RequestDecoder();
    private static final ProtobufDecoder protobufDecoder = new ProtobufDecoder(Message.MessageDemo.getDefaultInstance());

    public HttpHelloWorldServerInitializer(SslContext sslCtx) {
        this.sslCtx = sslCtx;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null) {
            p.addLast(sslCtx.newHandler(ch.alloc()));
        }
      /*  p.addLast(new HttpObjectAggregator());

        p.addLast(new ProtobufVarint32FrameDecoder());
                  p.addLast(new ProtobufDecoder(Message.MessageDemo.getDefaultInstance()));

                  p.addLast(new ProtobufVarint32LengthFieldPrepender());
                  p.addLast(new ProtobufEncoder());*/

        p.addLast("httpDecoder", new HttpServerCodec());
        // Don't want to handle HttpChunks
//p.addLast("httpAggregator",
         //     new HttpObjectAggregator(4096));
        p.addLast("httpDecoderAux", requestDecoder);
     //   p.addLast("httpEncoder", new HttpResponseEncoder());
        // Automatic content compression
//		p.addLast("httpDeflater", new HttpContentCompressor());
     //p.addLast("httpPayloadDecoder", new FormPayloadDecoder());
        //p.addLast(protobufDecoder);
        //p.addLast("handler", new DefaultHandler(executor));
        //p.addLast("httpExceptionHandler", new DefaultExceptionHandler());
        //p.addLast("proto",new ProtobufDecoder(Message.MessageDemo.getDefaultInstance()));
        p.addLast(handler);
    }
}