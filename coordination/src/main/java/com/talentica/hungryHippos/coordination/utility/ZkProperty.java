/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author pooshans
 *
 */
public class ZkProperty extends CommonProperty<ZkProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZkProperty.class.getName());

  public ZkProperty(String propFileName) {
    super(propFileName);
  }

}
