/**
 * 
 */
package com.talentica.hungryHippos.job.context;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.job.property.JobManagerProperty;



/**
 * @author pooshans
 * @param <T>
 *
 */
public class JobManagerApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobManagerApplicationContext.class);

  private static Property<JobManagerProperty> property;
  private static NodesManager nodesManager;
  public static final String SERVER_CONF_FILE = "serverConfigFile.properties";
  private static FieldTypeArrayDataDescription dataDescription;

  private static Properties serverProp = null;

  public static Property<JobManagerProperty> getProperty() {
    if (property == null) {
      property = new JobManagerProperty("jobmanager-config.properties");
    }
    return property;
  }

  public static NodesManager getNodesManager() throws Exception {
    nodesManager = NodesManagerContext.getNodesManagerInstance();
    return nodesManager;
  }
}
