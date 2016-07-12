package com.talentica.hungryHippos.coordination;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

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
      String coordinationConfigFilePath = args[0];
      String zookeeperConfigFilePath = args[1];
      String shardingConfigFilePath = args[2];
      String jobConfigFilePathString = args[3];
      CoordinationApplicationContext.setCoordinationConfigPathContext(coordinationConfigFilePath);
      CoordinationConfig configuration = CoordinationApplicationContext.getCoordinationConfig();
      if (configuration.getClusterConfig() == null
          || configuration.getClusterConfig().getNode().isEmpty()) {
        throw new RuntimeException("Invalid configuration file or cluster configuration missing."
            + coordinationConfigFilePath);
      }
      NodesManagerContext.initialize(zookeeperConfigFilePath).startup();
      CoordinationApplicationContext.uploadConfigurationOnZk(
          CoordinationApplicationContext.COORDINATION_CONFIGURATION,
          FileUtils.readFileToString(new File(coordinationConfigFilePath), "UTF-8"));
      CoordinationApplicationContext.uploadConfigurationOnZk(
          CoordinationApplicationContext.ZOOKEEPER_CONFIGURATION,
          FileUtils.readFileToString(new File(zookeeperConfigFilePath), "UTF-8"));
      CoordinationApplicationContext.uploadConfigurationOnZk(
          CoordinationApplicationContext.SHARDING_CONFIGURATION,
          FileUtils.readFileToString(new File(shardingConfigFilePath), "UTF-8"));
      CoordinationApplicationContext.uploadConfigurationOnZk(
          CoordinationApplicationContext.JOB_CONFIGURATION,
          FileUtils.readFileToString(new File(jobConfigFilePathString), "UTF-8"));
      LOGGER.info("Coordination server started..");
    } catch (Exception exception) {
      LOGGER.error("Error occurred while starting coordination server.", exception);
      throw new RuntimeException(exception);
    }
  }

  private static void validateArguments(String[] args) {
    if (args == null || args.length < 4) {
      LOGGER
          .error("Either missing 1st argument {coordination configuration} or 2nd argument {zookeeper configuration} or 3rd argument {sharding configuration}  or 4th argument {job configuration} file/files arguments.");
      System.exit(1);
    }
  }

}
