package com.talentica.hungryHippos.common.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryhippos.config.jobrunner.JobRunnerConfig;

/**
 * {@code JobRunnerApplicationContext} used for retrieving Job related Configuration file from
 * Zookeeper.
 * 
 * @author
 *
 */
public class JobRunnerApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunnerApplicationContext.class);
  private static JobRunnerConfig jobRunnerConfig;

  /**
   * 
   * @return a new JobRunnerConfig Object, Object is created from the Zookeeper nodes.
   */
  public static JobRunnerConfig getZkJobRunnerConfig() {
    if (jobRunnerConfig == null) {

      String configFile =
          CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path") + "/"
              + CoordinationConfigUtil.JOB_RUNNER_CONFIGURATION;
      try {
        jobRunnerConfig =
            (JobRunnerConfig) HungryHippoCurator.getInstance().readObject(configFile);
      } catch (HungryHippoException e) {
        LOGGER.error(e.getMessage());
      }

    }
    return jobRunnerConfig;
  }
}
