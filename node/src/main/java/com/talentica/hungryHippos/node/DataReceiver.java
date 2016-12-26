package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.node.datareceiver.FileIdHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.context.DataPublisherApplicationContext;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 
 * {@code DataReceiver } used for receiving data from client side.
 */
public class DataReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataReceiver.class.getName());
  public static final String FILE_ID_HANDLER = "FILE_ID_HANDLER";
  public static final String DATA_HANDLER = "DATA_HANDLER";
  private static String userName;

  private int port;

  public DataReceiver(int port) {
    this.port = port;
  }

  public static String getUserName() {
    return userName;
  }

  /**
   * It will open the port to accept the sharded data from client.
   * 
   * @throws Exception
   */
  private void startServer() {
    LOGGER.info("Start the node");
    int noOfNodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode().size();

    int maxNoOfRequestThreads =
        noOfNodes * DataPublisherApplicationContext.getNoOfDataReceiverThreads();

    EventLoopGroup workerGroup = new NioEventLoopGroup(maxNoOfRequestThreads);
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(workerGroup);
      b.channel(NioServerSocketChannel.class);
      b.option(ChannelOption.SO_KEEPALIVE, true);
      b.childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline pipeline = ch.pipeline();
          pipeline.addLast(FILE_ID_HANDLER, new FileIdHandler());
        }
      });
      LOGGER.info("binding to port " + port);
      ChannelFuture f = b.bind(port).sync();
      f.channel().closeFuture().sync();
      LOGGER.info("Node ready to receive data");
      LOGGER.info("Wait until the connection is closed");
    } catch (Exception e) {
      LOGGER.error(e.toString());
    } finally {
      workerGroup.shutdownGracefully();
      LOGGER.info("Connection is gracefully closed");
    }
  }

  public static void main(String[] args) {
    try {
      String hungryHippoBinDir = System.getProperty("hh.bin.dir");
      if(hungryHippoBinDir==null){
        throw new RuntimeException("System property hh.bin.dir is not set. Set the property value to HungryHippo bin Directory");
      }
      validateArguments(args);
      ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(args[0], ClientConfig.class);
      String connectString = clientConfig.getCoordinationServers().getServers();
      int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
      userName = clientConfig.getOutput().getNodeSshUsername();
      HungryHippoCurator.getInstance(connectString, sessionTimeOut);
      LOGGER.info("Start Node initialize");
      int nodePort = NodeInfo.INSTANCE.getPort();
      DataReceiver dataReceiver = new DataReceiver(nodePort);
      dataReceiver.startServer();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      throw new RuntimeException("Please provide client-config.xml to connect to zookeeper.");
    }
  }

}
