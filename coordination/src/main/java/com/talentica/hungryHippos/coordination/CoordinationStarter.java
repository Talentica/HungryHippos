package com.talentica.hungryHippos.coordination;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;

/**
 * Starts coordination server and updates configuration about cluster environment.
 * 
 * @author nitink
 *
 */
public class CoordinationStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinationStarter.class);

  public static void main(String[] args) {
    try {
      LOGGER.info("Starting coordination server..");
      validateArguments(args);
      String clientConfigFilePath = args[0];
      String coordinationConfigFilePath = args[1];
      String clusterConfigFilePath = args[2];
      String datapublisherConfigFilePath = args[3];
      String fileSystemConfigFilePath = args[4];
      String jobRunnerConfigFilePath = args[5];
      validateFileSystem(fileSystemConfigFilePath);
      CoordinationConfigUtil.setLocalClusterConfigPath(clusterConfigFilePath);
      ClusterConfig configuration =
          JaxbUtil.unmarshalFromFile(clusterConfigFilePath, ClusterConfig.class);
      if (configuration.getNode().isEmpty()) {
        throw new RuntimeException("Invalid configuration file or cluster configuration missing."
            + coordinationConfigFilePath);
      }

      NodesManager manager = NodesManagerContext.initialize(clientConfigFilePath);

      CoordinationConfig coordinationConfig =
          JaxbUtil.unmarshalFromFile(coordinationConfigFilePath, CoordinationConfig.class);
      manager.initializeZookeeperDefaultConfig(coordinationConfig.getZookeeperDefaultConfig());
      manager.startup();
      CoordinationConfigUtil.uploadConfigurationOnZk(manager,
          CoordinationConfigUtil.COORDINATION_CONFIGURATION,
          FileUtils.readFileToString(new File(coordinationConfigFilePath), "UTF-8"));
      CoordinationConfigUtil.uploadConfigurationOnZk(manager,
          CoordinationConfigUtil.CLUSTER_CONFIGURATION,
          FileUtils.readFileToString(new File(clusterConfigFilePath), "UTF-8"));
      CoordinationConfigUtil.uploadConfigurationOnZk(manager,
          CoordinationConfigUtil.CLIENT_CONFIGURATION,
          FileUtils.readFileToString(new File(clientConfigFilePath), "UTF-8"));
      CoordinationConfigUtil.uploadConfigurationOnZk(manager,
          CoordinationConfigUtil.DATA_PUBLISHER_CONFIGURATION,
          FileUtils.readFileToString(new File(datapublisherConfigFilePath), "UTF-8"));
      CoordinationConfigUtil.uploadConfigurationOnZk(manager,
          CoordinationConfigUtil.FILE_SYSTEM,
          FileUtils.readFileToString(new File(fileSystemConfigFilePath), "UTF-8"));
      CoordinationConfigUtil.uploadConfigurationOnZk(manager,
          CoordinationConfigUtil.JOB_RUNNER_CONFIGURATION,
          FileUtils.readFileToString(new File(jobRunnerConfigFilePath), "UTF-8"));


      LOGGER.info("Coordination server started..");
    } catch (Exception exception) {
      LOGGER.error("Error occurred while starting coordination server.", exception);
      throw new RuntimeException(exception);
    }
  }

  private static void validateArguments(String[] args) {
    if (args == null || args.length < 6) {
      LOGGER.error(
          "Either missing 1st argument {client configuration} or 2nd argument {coordination configuration} or 3rd argument {cluster configuration} or 4th argument {datapubliser configuration} or 5th argument {file system configuration} or 6th argument {jobrunner configuration}file/files arguments.");
      System.exit(1);
    }
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

}
