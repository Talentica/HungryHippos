/**
 * 
 */
package com.talentica.hungryHippos.master.context;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.utility.ServerProperty;
import com.talentica.hungryHippos.master.property.DataPublisherProperty;
import com.talentica.hungryhippos.config.client.CoordinationServers;



/**
 * @author pooshans
 * @param <T>
 *
 */
public class DataPublisherApplicationContext {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(DataPublisherApplicationContext.class);

  private static Property<DataPublisherProperty> property;
  private static NodesManager nodesManager;
  public static final String SERVER_CONF_FILE = "serverConfigFile.properties";
  private static Property<ServerProperty> serverProperty;
  private static FieldTypeArrayDataDescription dataDescription;
  public static String inputFile;

  private static Properties serverProp = null;

  public static Property<DataPublisherProperty> getProperty() {
    if (property == null) {
      property = new DataPublisherProperty("datapublisher-config.properties");
    }
    return property;
  }

	public static NodesManager getNodesManager(CoordinationServers coordinationServers) throws Exception {
		nodesManager = NodesManagerContext.getNodesManagerInstance(coordinationServers);
		return nodesManager;
	}

  public static final FieldTypeArrayDataDescription getConfiguredDataDescription() {
    if (dataDescription == null) {
      dataDescription =
          FieldTypeArrayDataDescription.createDataDescription(getDataTypeConfiguration(),
              getMaximumSizeOfSingleDataBlock());
    }
    return dataDescription;
  }

  public static Property<ServerProperty> getServerProperty() {
    if (serverProperty == null) {
      serverProperty = new ServerProperty("server-config.properties");
    }
    return serverProperty;
  }

  public static String[] getShardingDimensions() {
    getProperty();
    String keyOrderString = property.getValueByKey("sharding_dimensions").toString();
    return keyOrderString.split(",");
  }

  public final static String[] getDataTypeConfiguration() {
    getProperty();
    return property.getValueByKey("column.datatype-size").toString().split(",");
  }

  public final static int getMaximumSizeOfSingleDataBlock() {
    getProperty();
    return Integer.parseInt(property.getValueByKey("maximum.size.of.single.block.data"));
  }
}
