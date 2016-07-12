/**
 * 
 */
package com.talentica.hungryHippos.job.context;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.job.JobConfig;


/**
 * @author pooshans
 * @param <T>
 *
 */
public class JobManagerApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobManagerApplicationContext.class);

  private static String jobConfigFilePath;
  private static JobConfig jobConfig;


  public static JobConfig getJobConfig() throws FileNotFoundException, JAXBException {
    if (jobConfig != null) {
      return jobConfig;
    }
    if (JobManagerApplicationContext.jobConfigFilePath == null) {
      LOGGER.info("Please set the job configuration file path.");
      return null;
    }
    jobConfig =
        JaxbUtil.unmarshalFromFile(JobManagerApplicationContext.jobConfigFilePath, JobConfig.class);
    return jobConfig;
  }

  public static void setJobConfigPathContext(String jobConfigFilePath) {
    JobManagerApplicationContext.jobConfigFilePath= jobConfigFilePath;
  }


}
