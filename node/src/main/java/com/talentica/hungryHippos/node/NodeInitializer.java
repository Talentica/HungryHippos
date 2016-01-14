package com.talentica.hungryHippos.node;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Created by debasishc on 1/9/15.
 */
public class NodeInitializer {

	private static final Logger LOGGER = LoggerFactory.getLogger(NodeInitializer.class);

    private Map<String,Map<Object, Node>> keyValueNodeNumberMap ;
    private DataDescription dataDescription;
    private static String nodeIdFile = "nodeId";

    @SuppressWarnings("unchecked")
	public NodeInitializer(String keyValueNodeNumberMapFile, DataDescription dataDescription) throws IOException {
        this.dataDescription = dataDescription;
        try(ObjectInputStream in
                    = new ObjectInputStream(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+keyValueNodeNumberMapFile))){
            keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) in.readObject();
        } catch (ClassNotFoundException e) {
			LOGGER.error("Error while reading keyValueNodeNumberMap object.", e);
			;
        }
    }
    public static int readNodeId() throws Exception{
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+nodeIdFile)));
        String line = in.readLine();
        return Integer.parseInt(line);
    }
    public void startServer(int port, int hostID) throws Exception {

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        EventLoopGroup bossGroup = new NioEventLoopGroup();

        try {

            //need to setup this
            final NodeDataStoreIdCalculator nodeDataStoreIdCalculator
                    = new NodeDataStoreIdCalculator(keyValueNodeNumberMap,hostID,dataDescription);
            final DataStore dataStore
                    = new FileDataStore(keyValueNodeNumberMap.size(),nodeDataStoreIdCalculator,dataDescription);

            ServerBootstrap b = new ServerBootstrap(); // (1)
            b.group(bossGroup,workerGroup); // (2)
            b.channel(NioServerSocketChannel.class); // (3)
            b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
            b.childHandler(new ChannelInitializer<SocketChannel>() {

                @Override
                protected void initChannel(SocketChannel ch) throws Exception {

                    ch.pipeline().addLast(new DataReadHandler(dataDescription, dataStore));
                }
            });
			LOGGER.info("Node listening on port {}", port);
            ChannelFuture f = b.bind(port).sync(); // (5)
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception{
        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.DOUBLE,0);
        dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 0);
        dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});
        NodeInitializer initializer = new NodeInitializer("keyValueNodeNumberMap", dataDescription);
        initializer.startServer(2323, readNodeId());
    }
}
