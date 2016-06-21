/**
 * 
 */
package com.talentica.hungryHippos.node.test;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.node.NodeProperty;

/**
 * @author pooshans
 *
 */

public class PropertyTest {
  
  @Test
  public void testProperty() throws IOException {
    Property<NodeProperty> property = new NodeProperty("config.properties");
    Properties properties = property.getProperties();
  }

}
