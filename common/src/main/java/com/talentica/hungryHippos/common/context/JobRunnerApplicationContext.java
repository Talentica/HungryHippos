package com.talentica.hungryHippos.common.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryhippos.config.jobrunner.JobRunnerConfig;

public class JobRunnerApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunnerApplicationContext.class);
  private static JobRunnerConfig jobRunnerConfig;

  public static JobRunnerConfig getZkJobRunnerConfig() {
    if (jobRunnerConfig == null) {
     
        String configFile =  CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path")
            + ZkUtils.zkPathSeparator + CoordinationConfigUtil.JOB_RUNNER_CONFIGURATION;
        jobRunnerConfig = (JobRunnerConfig) ZkUtils.readObjectZkNode(configFile);
      
    }
    return jobRunnerConfig;
  }
}
