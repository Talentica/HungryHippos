/**
 * 
 */
package com.talentica.hungryHippos.master.property;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.property.Property;

/**
 * @author pooshans
 *
 */
public class DataPublisherProperty extends Property<DataPublisherProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherProperty.class
      .getName());

  public DataPublisherProperty(String propFileName) {
    super(propFileName);
  }

  public DataPublisherProperty() {

  }

}
