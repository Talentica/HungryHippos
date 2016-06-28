/**
 * 
 */
package com.talentica.hungryHippos.job.property;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.property.Property;

/**
 * @author pooshans
 *
 */
public class JobManagerProperty extends Property<JobManagerProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobManagerProperty.class
      .getName());

  public JobManagerProperty(String propFileName) {
    super(propFileName);
  }

  public JobManagerProperty() {

  }

}
