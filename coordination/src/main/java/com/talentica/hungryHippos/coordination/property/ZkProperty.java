/**
 * 
 */
package com.talentica.hungryHippos.coordination.property;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author pooshans
 *
 */
public class ZkProperty extends Property<ZkProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZkProperty.class.getName());

  public ZkProperty(String propFileName) {
    super(propFileName);
  }

  public ZkProperty() {

  }

}
