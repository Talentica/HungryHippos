/**
 * 
 */
package com.talentica.hungryHippos.node.property;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.property.Property;

/**
 * @author pooshans
 *
 */
public class NodeProperty extends Property<NodeProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeProperty.class.getName());

  public NodeProperty(String propFileName) {
    super(propFileName);
  }
}
