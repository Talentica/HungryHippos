package com.talentica.hungryHippos.common.context;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.jobrunner.JobRunnerConfig;

public class JobRunnerApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunnerApplicationContext.class);
  private static JobRunnerConfig jobRunnerConfig;

  public static JobRunnerConfig getZkJobRunnerConfig() {
    if (jobRunnerConfig == null) {
      try {
        ZKNodeFile configFile = (ZKNodeFile) NodesManagerContext.getNodesManagerInstance()
            .getConfigFileFromZNode(CoordinationApplicationContext.JOB_RUNNER_CONFIGURATION);
        jobRunnerConfig = JaxbUtil.unmarshal((String) configFile.getObj(), JobRunnerConfig.class);
      } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
        LOGGER.info("Please upload the job-runner configuration file on zookeeper");
      } catch (JAXBException e1) {
        LOGGER.info("Unable to unmarshal the job-runner xml configuration.");
      }
    }
    return jobRunnerConfig;
  }
}
