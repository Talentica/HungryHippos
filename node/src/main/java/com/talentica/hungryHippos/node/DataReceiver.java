package com.talentica.hungryHippos.node;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
import com.talentica.hungryhippos.config.coordination.Node;

public class DataReceiver {

  private DataDescription dataDescription;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataReceiver.class.getName());

  private DataStore dataStore;

  public DataReceiver(DataDescription dataDescription) throws Exception {
    this.dataDescription = dataDescription;
    dataStore = new FileDataStore(NodeUtil.getKeyToValueToBucketMap().size(), dataDescription);
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
      validateArguments(args);
      setContext(args);
      LOGGER.info("Start Node initialize");
      long startTime = System.currentTimeMillis();
      DataReceiver dataReceiver = getNodeInitializer();
      CoordinationConfig coordinationConfig =
          CoordinationApplicationContext.getZkCoordinationConfigCache();
      List<Node> nodes = coordinationConfig.getClusterConfig().getNode();
      int nodeId = NodeUtil.getNodeId();
      int PORT = 0;

      for (Node node : nodes) {
        if (node.getIdentifier() == nodeId) {
          PORT = Integer.valueOf(node.getPort());
          break;
        }
      }
      dataReceiver.startServer(PORT, nodeId);
      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to for receiving all data on this node.",
          ((endTime - startTime) / 1000));
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
      throw new RuntimeException(
          "Missing zookeeper xml configuration file path arguments.");
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
    FieldTypeArrayDataDescription dataDescription =
        CoordinationApplicationContext.getConfiguredDataDescription();
    dataDescription.setKeyOrder(CoordinationApplicationContext.getShardingDimensions());
    return new DataReceiver(dataDescription);
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

  private static void setContext(String[] args) {
    NodesManagerContext.setZookeeperXmlPath(args[0]);
  }

}
