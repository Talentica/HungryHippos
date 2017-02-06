package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rajkishoreh on 23/11/16.
 */
public class DataDistributorStarter {

  public static ExecutorService dataDistributorService;
  public static ExecutorService fileProviderService;
  public static ExecutorService dataAppenderServices;
  public static ExecutorService scpAccessServices;
  public static ExecutorService metadataUpdaterServices;
  public static ExecutorService metadataSynchronizerServices;
  public static ClientConfig clientConfig;


  public static void main(String[] args) throws Exception {
    validateArguments(args);

    clientConfig = JaxbUtil.unmarshalFromFile(args[0], ClientConfig.class);
    String connectString = clientConfig.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
    HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    int noOfNodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode().size();
    ServerSocket serverSocket = new ServerSocket(8789);
    ExecutorService serviceDeligator = Executors.newFixedThreadPool(10);
    dataDistributorService = Executors.newFixedThreadPool(4);
    fileProviderService = Executors.newFixedThreadPool(10);
    dataAppenderServices = Executors.newFixedThreadPool(1);
    scpAccessServices = Executors.newFixedThreadPool(1);
    metadataUpdaterServices = Executors.newFixedThreadPool(1);
    metadataSynchronizerServices = Executors.newFixedThreadPool(1);
    while (true) {
      Socket socket = serverSocket.accept();
      serviceDeligator.execute(new ServiceDeligator(socket));
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      throw new RuntimeException("Please provide client-config.xml to connect to zookeeper.");
    }
  }


}
