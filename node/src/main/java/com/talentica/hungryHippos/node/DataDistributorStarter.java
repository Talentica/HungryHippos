/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.service.CacheClearService;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rajkishoreh on 23/11/16.
 */
public class DataDistributorStarter {

  private static final Logger logger = LoggerFactory.getLogger(DataDistributorStarter.class);
  public static ExecutorService dataDistributorService;
  public static ExecutorService fileProviderService;
  public static ExecutorService dataAppenderServices;
  public static ExecutorService publishAccessServices;
  public static ExecutorService metadataUpdaterServices;
  public static ExecutorService metadataSynchronizerServices;
  public static ExecutorService cacheClearServices;
  public static ExecutorService fileService;
  public static ExecutorService commonServicePoolCache;
  public static ExecutorService fileJoinerServices;
  public static AtomicInteger noOfAvailableDataDistributors;
  public static int noOfDataDistributors;
  public static ClientConfig clientConfig;


  public static void main(String[] args) throws Exception {
    validateArguments(args);

    clientConfig = JaxbUtil.unmarshalFromFile(args[0], ClientConfig.class);
    String connectString = clientConfig.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
    HungryHippoCurator hungryHippoCurator= HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    ServerSocket serverSocket = new ServerSocket(NodeInfo.INSTANCE.getPort());

    noOfDataDistributors = 4;
    commonServicePoolCache = Executors.newCachedThreadPool();
    fileJoinerServices = Executors.newFixedThreadPool(1);
    ExecutorService serviceDelegator = commonServicePoolCache;
    dataDistributorService = commonServicePoolCache;
    fileProviderService = commonServicePoolCache;
    publishAccessServices = commonServicePoolCache;
    dataAppenderServices = commonServicePoolCache;
    metadataUpdaterServices = commonServicePoolCache;
    metadataSynchronizerServices = Executors.newWorkStealingPool(1);
    fileService = commonServicePoolCache;
    cacheClearServices = commonServicePoolCache;
    noOfAvailableDataDistributors = new AtomicInteger(noOfDataDistributors);

    String ephemeralNode = CoordinationConfigUtil.getHostsPath()
              + HungryHippoCurator.ZK_PATH_SEPERATOR + NodeInfo.INSTANCE.getIp();
    hungryHippoCurator.deletePersistentNodeIfExits(ephemeralNode);

    hungryHippoCurator.createEphemeralNode(ephemeralNode);
    logger.info("DataDistributor Application started");
    DataDistributorStarter.cacheClearServices.execute(new CacheClearService());
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
