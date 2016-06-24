/**
 * 
 */
package com.talentica.hungryHippos.node.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.node.property.NodeProperty;



/**
 * @author pooshans
 * @param <T>
 *
 */
public class NodeApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeApplicationContext.class);

  private static Property<NodeProperty> property;
  private static NodesManager nodesManager;
  public static final String SERVER_CONF_FILE = "serverConfigFile.properties";
  private static FieldTypeArrayDataDescription dataDescription;

  public static Property<NodeProperty> getProperty() {
    if (property == null) {
      property = new NodeProperty("node-config.properties");
    }
    return property;
  }

  public static NodesManager getNodesManager() throws Exception {
    nodesManager = NodesManagerContext.getNodesManagerInstance();
    return nodesManager;
  }
}
