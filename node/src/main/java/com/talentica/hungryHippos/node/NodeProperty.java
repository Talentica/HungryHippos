/**
 * 
 */
package com.talentica.hungryHippos.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.utility.CommonProperty;

/**
 * @author pooshans
 *
 */
public class NodeProperty extends CommonProperty<NodeProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeProperty.class.getName());

  public NodeProperty(String propFileName) {
    super(propFileName);
  }
}
