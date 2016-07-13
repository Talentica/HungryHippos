/**
 * 
 */
package com.talentica.hungryHippos.job.context;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.job.JobConfig;


/**
 * @author pooshans
 * @param <T>
 *
 */
public class JobManagerApplicationContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobManagerApplicationContext.class);
  private static JobConfig jobConfig;

  public static JobConfig getZkJobConfigCache() {
    if (jobConfig != null) {
      return jobConfig;
    }
    ZKNodeFile configurationFile;
    try {
      configurationFile =
          (ZKNodeFile) NodesManagerContext.getNodesManagerInstance().getConfigFileFromZNode(
              CoordinationApplicationContext.JOB_CONFIGURATION);
      JobConfig jobConfig =
          JaxbUtil.unmarshal((String) configurationFile.getObj(), JobConfig.class);
      JobManagerApplicationContext.jobConfig = jobConfig;
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
      LOGGER.info("Please upload the sharding configuration file on zookeeper");
    } catch (JAXBException e1) {
      LOGGER.info("Unable to unmarshal the sharding xml configuration.");
    }
    return JobManagerApplicationContext.jobConfig;
  }


}
