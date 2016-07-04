/**
 * 
 */
package com.talentica.hungryHippos.job.context;

import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.job.property.JobManagerProperty;

/**
 * @author pooshans
 * @param <T>
 *
 */
public class JobManagerApplicationContext {

	private static Property<JobManagerProperty> property;

	public static final String SERVER_CONF_FILE = "serverConfigFile.properties";

	public static Property<JobManagerProperty> getProperty() {
		if (property == null) {
			property = new JobManagerProperty("jobmanager-config.properties");
		}
		return property;
	}

}
