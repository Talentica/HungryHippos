package com.talentica.hungryHippos.node;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.ZKNodeName;

public class DataReceiver {

	private DataDescription dataDescription;

	private static final Logger LOGGER = LoggerFactory.getLogger(DataReceiver.class.getName());

	private DataStore dataStore;

	public DataReceiver(DataDescription dataDescription) throws Exception {
		this.dataDescription = dataDescription;
		dataStore = new FileDataStore(NodeUtil.getKeyToValueToBucketMap().size(),
				dataDescription);
	}

	/**
	 * It will open the port to accept the sharded data from client.
	 * 
	 * @param port
	 * @param nodeId
	 * @throws Exception
	 */
	private void startServer(int port, int nodeId) throws Exception {
		LOGGER.info("Start the node");
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup);
			b.channel(NioServerSocketChannel.class);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			b.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline().addLast(new DataReadHandler(dataDescription, dataStore));
				}
			});
			LOGGER.info("binding to port " + port);
			ChannelFuture f = b.bind(port).sync();
			f.channel().closeFuture().sync();
			LOGGER.info("Node ready to receive data");
			LOGGER.info("Wait until the connection is closed");
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
			LOGGER.info("Connection is gracefully closed");
		}
	}

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			//validateArguments(args);
			Property.initialize(PROPERTIES_NAMESPACE.NODE);
			NodesManager nodesManager = CommonUtil.connectZK();
			
			CountDownLatch signal = new CountDownLatch(1);
			ZKUtils.waitForSignal(nodesManager.buildAlertPathByName(ZKNodeName.START_NODE_FOR_DATA_RECIEVER), signal);
			signal.await();
			
			DataReceiver dataReceiver = getNodeInitializer();
			ZKNodeFile serverConfig = ZKUtils.getConfigZKNodeFile(Property.SERVER_CONF_FILE);
			int nodeId = NodeUtil.getNodeId();
			String server = serverConfig.getFileData().getProperty("server." + nodeId);
			int PORT = Integer.valueOf(server.split(":")[1]);
			LOGGER.info("Start Node initialize");
			dataReceiver.startServer(PORT, nodeId);
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to for receiving all data on this node.",
					((endTime - startTime) / 1000));
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing node starter program.", exception);
		}
	}

	/**
	 * To validate the argument command line.
	 * 
	 * @param args
	 * @throws IOException
	 */
	private static void validateArguments(String[] args) throws IOException, FileNotFoundException {
		if (args.length == 1) {
			Property.overrideConfigurationProperties(args[0]);
		} else {
			System.out.println("Please provide the zookeeper configuration file");
			System.exit(1);
		}
	}
	
	
	/**
	 * Initialize the node.
	 * 
	 * @param nodesManager
	 * @return NodeStarter
	 * @throws Exception
	 */
	private static DataReceiver getNodeInitializer() throws Exception {
		FieldTypeArrayDataDescription dataDescription = CommonUtil.getConfiguredDataDescription();
		dataDescription.setKeyOrder(Property.getShardingDimensions());
		return new DataReceiver(dataDescription);
	}

}
