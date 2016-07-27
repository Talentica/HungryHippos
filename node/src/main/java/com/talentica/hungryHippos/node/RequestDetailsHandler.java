package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by rajkishoreh on 22/7/16.
 */
public class RequestDetailsHandler extends ChannelHandlerAdapter {

	private String nodeId;

	public RequestDetailsHandler(String nodeId) {
		super();
		this.nodeId = nodeId;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		String relativePath = (String) msg;
		byte[] msgBytes = relativePath.getBytes();

		char[] intCharacters = new char[2];
		for (int i = 0; i < 2; i++) {
			intCharacters[i] = (char) msgBytes[i];

		}
		int size = Integer.parseInt(String.valueOf(intCharacters));

		char[] outPutDir = new char[size];
		for (int k = 0, j = 2; j < size + 2; j++) {
			outPutDir[k++] = (char) msgBytes[j];
		}
		relativePath = String.valueOf(outPutDir);
		ctx.pipeline().remove(DataReceiver.STRING_DECODER);
		ctx.pipeline().remove(DataReceiver.REQUEST_DETAILS_HANDLER);
		// TODO Get dataDescription for the particular file from zookeeper
		// instead of using common config
		DataDescription dataDescription = CoordinationApplicationContext.getConfiguredDataDescription();
		// TODO Get sharding table for the particular file from zookeeper
		// instead of using common config
		DataStore dataStore = new FileDataStore(NodeUtil.getKeyToValueToBucketMap().size(), dataDescription,
				relativePath, nodeId);
		ctx.pipeline().addLast(DataReceiver.DATA_HANDLER, new DataReadHandler(dataDescription, dataStore));
	}
}
