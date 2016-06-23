/**
 * 
 */
package com.talentica.hungryHippos.master.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;



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
  private static FieldTypeArrayDataDescription dataDescription;

  private static Properties serverProp = null;

  public static Property<DataPublisherProperty> getProperty() {
    if (property == null) {
      property = new DataPublisherProperty("datapublisher-config.properties");
    }
    return property;
  }

  public static NodesManager getNodesManager() throws Exception {
    nodesManager = NodesManagerContext.getNodesManagerInstance();
    return nodesManager;
  }

  public static void setNodesManager(NodesManager nodesManager) {
    DataPublisherApplicationContext.nodesManager = nodesManager;
  }

  public static final FieldTypeArrayDataDescription getConfiguredDataDescription() {
    if (dataDescription == null) {
      dataDescription =
          FieldTypeArrayDataDescription.createDataDescription(getDataTypeConfiguration(),
              getMaximumSizeOfSingleDataBlock());
    }
    return dataDescription;
  }

  public static Properties loadServerProperties() {
    if (serverProp == null) {
      serverProp = new Properties();
      try {
        InputStream is =
            new FileInputStream(CommonUtil.TEMP_JOBUUID_FOLDER_PATH + SERVER_CONF_FILE);
        serverProp.load(is);
        PropertyConfigurator.configure(serverProp);
        LOGGER.info("serverConfigFile.properties file is loaded");
      } catch (IOException e) {
        LOGGER.warn("Unable to load serverConfigFile.properties file");
      }
    }
    return serverProp;
  }

  public static String[] getShardingDimensions() {
    getProperty();
    String keyOrderString = property.getValueByKey("common.sharding_dimensions").toString();
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