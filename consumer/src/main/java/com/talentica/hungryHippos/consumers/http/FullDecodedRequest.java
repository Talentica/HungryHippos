package com.talentica.hungryHippos.consumers.http;

import io.netty.handler.codec.http.QueryStringDecoder;


public class FullDecodedRequest {

    private final Request request;



    public FullDecodedRequest(Request request) {
        this.request = request;

    }

    public Request getRequest() {
        return request;
    }

    public String getPath() {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(
                request.getHttpRequest().getUri());
        return queryStringDecoder.path();
    }
}
