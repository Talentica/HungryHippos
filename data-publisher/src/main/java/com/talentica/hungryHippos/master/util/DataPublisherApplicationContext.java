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

  private static Property<?> property;
  private static NodesManager nodesManager;
  public static final String SERVER_CONF_FILE = "serverConfigFile.properties";
  private static FieldTypeArrayDataDescription dataDescription;

  private static Properties serverProp = null;

  public static Property<?> getProperty() {
    if (property == null) {
      property = new DataPublisherProperty("config.properties");
    }
    return property;
  }

  public static NodesManager getNodesManager() {
    return nodesManager;
  }

  public static void setNodesManager(NodesManager nodesManager) {
    DataPublisherApplicationContext.nodesManager = nodesManager;
  }

  public static final FieldTypeArrayDataDescription getConfiguredDataDescription() {
    if (dataDescription == null) {
      dataDescription =
          FieldTypeArrayDataDescription.createDataDescription(
              ((DataPublisherProperty) property).getDataTypeConfiguration(),
              ((DataPublisherProperty) property).getMaximumSizeOfSingleDataBlock());
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
    String keyOrderString = property.getValueByKey("common.sharding_dimensions").toString();
    return keyOrderString.split(",");
  }

}
