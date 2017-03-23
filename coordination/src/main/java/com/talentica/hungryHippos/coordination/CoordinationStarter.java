/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.coordination;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.datapublisher.DatapublisherConfig;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;

/**
 * Starts coordination server and updates configuration about cluster environment.
 * 
 * @author nitink
 *
 */
public class CoordinationStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinationStarter.class);

  private static String clientConfigFilePath;
  private static String clusterConfigFilePath;
  private static String datapublisherConfigFilePath;
  private static String fileSystemConfigFilePath;

  private static ClusterConfig clusterConfig;

  /**
   * Execution entry point.
   * 
   * @param args
   */
  public static void main(String[] args) {

    try {
      LOGGER.info("Starting coordination server..");

      validateArguments(args);

      setArguments(args);

      validateFileSystem(fileSystemConfigFilePath);

      String rootPath = getAndValidateRoot();

      CoordinationConfigUtil.setLocalClusterConfigPath(clusterConfigFilePath);

      clusterConfig = JaxbUtil.unmarshalFromFile(clusterConfigFilePath, ClusterConfig.class);

      validateClusterIsnotEmpty(clusterConfig);

      startNodeManager();

      uploadConfigurationOnZk(rootPath);

      LOGGER.info("Coordination server started..");
    } catch (Exception exception) {
      LOGGER.error("Error occurred while starting coordination server.", exception);
      throw new RuntimeException(exception);
    }
  }

  private static void validateArguments(String[] args) {
    if (args == null || args.length < 4) {
      LOGGER.error(
          "Either missing 1st argument {client configuration} or 2nd argument {cluster configuration} or 3rd argument {datapubliser configuration} or 4th argument {file system configuration} file/files arguments.");
      System.exit(1);
    }
  }

  private static void setArguments(String... args) {
    clientConfigFilePath = args[0];
    clusterConfigFilePath = args[1];
    datapublisherConfigFilePath = args[2];
    fileSystemConfigFilePath = args[3];
  }

  private static void validateFileSystem(String fileSystemConfigFilePath)
      throws FileNotFoundException, JAXBException {
    // TODO output specific information and then exit
    FileSystemConfig configuration =
        JaxbUtil.unmarshalFromFile(fileSystemConfigFilePath, FileSystemConfig.class);
    if (configuration.getServerPort() == 0 || configuration.getFileStreamBufferSize() <= 0
        || configuration.getQueryRetryInterval() <= 0 || configuration.getMaxQueryAttempts() <= 0
        || configuration.getMaxClientRequests() <= 0 || configuration.getDataFilePrefix() == null
        || configuration.getRootDirectory() == null || "".equals(configuration.getDataFilePrefix())
        || "".equals(configuration.getRootDirectory())) {
      throw new RuntimeException("Invalid configuration file or cluster configuration missing."
          + fileSystemConfigFilePath);
    }
  }


  private static String getAndValidateRoot() {
    String rootPath = CoordinationConfigUtil.getConfigPath();
    rootPath = rootPath.endsWith(String.valueOf(File.separatorChar))
        ? rootPath.substring(0, rootPath.length() - 1) : rootPath;
    return rootPath;
  }


  private static void validateClusterIsnotEmpty(ClusterConfig clusterConfig) {
    if (clusterConfig.getNode().isEmpty()) {
      throw new RuntimeException("Invalid configuration file or cluster configuration missing."
          + clusterConfigFilePath);
    }
  }

  private static void startNodeManager() throws Exception {

    ClientConfig clientConfig =
        JaxbUtil.unmarshalFromFile(clientConfigFilePath, ClientConfig.class);
    String servers = clientConfig.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
    HungryHippoCurator curator = HungryHippoCurator.getInstance(servers, sessionTimeOut);
    curator.initializeZookeeperDefaultConfig();
      curator.startup();
  }



  private static void uploadConfigurationOnZk(String rootPath) throws FileNotFoundException,
      IOException, JAXBException, InterruptedException, HungryHippoException {

    CoordinationConfigUtil.uploadConfigurationOnZk(
        rootPath + File.separatorChar + CoordinationConfigUtil.CLUSTER_CONFIGURATION,
        clusterConfig);

    CoordinationConfigUtil.uploadConfigurationOnZk(
        rootPath + File.separatorChar + CoordinationConfigUtil.CLIENT_CONFIGURATION,
        JaxbUtil.unmarshalFromFile(clientConfigFilePath, ClientConfig.class));

    CoordinationConfigUtil.uploadConfigurationOnZk(
        rootPath + File.separatorChar + CoordinationConfigUtil.DATA_PUBLISHER_CONFIGURATION,
        JaxbUtil.unmarshalFromFile(datapublisherConfigFilePath, DatapublisherConfig.class));

    CoordinationConfigUtil.uploadConfigurationOnZk(
        rootPath + File.separatorChar + CoordinationConfigUtil.FILE_SYSTEM_CONFIGURATION,
        JaxbUtil.unmarshalFromFile(fileSystemConfigFilePath, FileSystemConfig.class));
  }


}
