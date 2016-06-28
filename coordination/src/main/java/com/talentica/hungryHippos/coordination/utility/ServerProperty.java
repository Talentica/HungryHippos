/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.property.Property;

/**
 * @author pooshans
 *
 */
public class ServerProperty extends Property<ServerProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerProperty.class.getName());

  public ServerProperty(String propFileName) {
    super(propFileName);
  }

  public ServerProperty() {

  }
}
