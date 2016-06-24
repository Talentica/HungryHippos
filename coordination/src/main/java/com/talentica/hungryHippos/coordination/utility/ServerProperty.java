/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.property.CommonProperty;

/**
 * @author pooshans
 *
 */
public class ServerProperty extends CommonProperty<ServerProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerProperty.class.getName());

  public ServerProperty(String propFileName) {
    super(propFileName);
  }

  public ServerProperty() {

  }
}
