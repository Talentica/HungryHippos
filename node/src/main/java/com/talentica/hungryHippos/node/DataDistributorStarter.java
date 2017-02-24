package com.talentica.hungryHippos.node;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;

/**
 * Created by rajkishoreh on 23/11/16.
 */
public class DataDistributorStarter {

  public static ExecutorService dataDistributorService;
  public static ExecutorService fileProviderService;
  public static ExecutorService dataAppenderServices;
  public static ExecutorService publishAccessServices;
  public static ExecutorService metadataUpdaterServices;
  public static ExecutorService metadataSynchronizerServices;
  public static ExecutorService fileService;
  public static AtomicInteger noOfAvailableDataDistributors;
  public static int noOfDataDistributors;
  public static ClientConfig clientConfig;


  public static void main(String[] args) throws Exception {
    validateArguments(args);

    clientConfig = JaxbUtil.unmarshalFromFile(args[0], ClientConfig.class);
    String connectString = clientConfig.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
    HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    ServerSocket serverSocket = new ServerSocket(NodeInfo.INSTANCE.getPort());
    ExecutorService serviceDelegator = Executors.newCachedThreadPool();
    noOfDataDistributors = 4;
    dataDistributorService = Executors.newCachedThreadPool();
    fileProviderService = Executors.newCachedThreadPool();
    dataAppenderServices = Executors.newFixedThreadPool(1);
    publishAccessServices = Executors.newFixedThreadPool(1);
    metadataUpdaterServices = Executors.newCachedThreadPool();
    metadataSynchronizerServices = Executors.newCachedThreadPool();
    fileService = Executors.newCachedThreadPool();
    noOfAvailableDataDistributors = new AtomicInteger(noOfDataDistributors);
    while (true) {
      Socket socket = serverSocket.accept();
      serviceDelegator.execute(new ServiceDelegator(socket));
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      throw new RuntimeException("Please provide client-config.xml to connect to zookeeper.");
    }
  }


}
