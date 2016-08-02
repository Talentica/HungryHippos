package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataReceiver {


  private static final Logger LOGGER = LoggerFactory.getLogger(DataReceiver.class.getName());

  public static final String REQUEST_DETAILS_HANDLER ="REQUEST_DETAILS_HANDLER";
  public static final String DATA_HANDLER="DATA_HANDLER";

  private int port;
  private String nodeId;

  public DataReceiver(int port, String nodeId) {
    this.port = port;
    this.nodeId = nodeId;
  }

  /**
   * It will open the port to accept the sharded data from client.
   * 
   * @throws Exception
   */
  private void startServer() throws Exception {
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
          ChannelPipeline pipeline = ch.pipeline();
          pipeline.addLast(REQUEST_DETAILS_HANDLER,new RequestDetailsHandler(nodeId));
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
      validateArguments(args);
      NodesManager manager = NodesManagerContext.getNodesManagerInstance(args[0]);
      CoordinationConfig coordinationConfig =
          CoordinationApplicationContext.getZkCoordinationConfigCache();
      manager.initializeZookeeperDefaultConfig(coordinationConfig.getZookeeperDefaultConfig());
      LOGGER.info("Start Node initialize");
      int nodePort = NodeInfo.INSTANCE.getPort();
      String nodeId = NodeInfo.INSTANCE.getId();
      DataReceiver dataReceiver = new DataReceiver(nodePort,nodeId);
      dataReceiver.startServer();
    } catch (Exception exception) {
      try {
        errorHandler(exception);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      throw new RuntimeException("Please provide client-config.xml to connect to zookeeper.");
    }
  }

  /**
   * @param exception
   * @throws Exception
   */
  private static void errorHandler(Exception exception) throws Exception {
    LOGGER.error("Error occured while executing node starter program.", exception);
    try {
      ZkSignalListener.createErrorEncounterSignal(NodesManagerContext.getNodesManagerInstance());
    } catch (IOException | InterruptedException e) {
      LOGGER.info("Unable to create the node on zk due to {}", e.getMessage());
    }
  }

}
