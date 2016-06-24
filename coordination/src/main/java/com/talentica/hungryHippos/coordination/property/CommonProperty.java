/**
 * 
 */
package com.talentica.hungryHippos.coordination.property;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author pooshans
 *
 */
public class CommonProperty<T> implements Property<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonProperty.class.getName());
  private static Properties properties = null;
  private static ClassLoader loader = null;
  private static InputStream CONFIG_FILE_INPUT_STREAM;

  public CommonProperty() {

  }

  public CommonProperty(String propFileName) {
    load(propFileName);
  }

  public Property<T> setPropertyFileName(String propFileName) {
    load(propFileName);
    return this;
  }

  public Property<T> overrideProperty(String propFileNameWithPath) throws IOException {
    CONFIG_FILE_INPUT_STREAM = new FileInputStream(propFileNameWithPath);
    if (properties == null) {
      properties = new Properties();
    }
    if (loader == null) {
      loader = getClassLoader();
    }
    properties.load(CONFIG_FILE_INPUT_STREAM);
    LOGGER.info(" {} property file is loaded.", propFileNameWithPath);
    return this;
  }



  private void loadProperties(String propFileName) throws IOException {
     if (properties == null) { 
    properties = new Properties();
     }
     if (loader == null) {
    loader = getClassLoader();
     }
    properties.load(loader.getResourceAsStream(propFileName));
    LOGGER.info(" {} property file is loaded.", propFileName);
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public String getValueByKey(String key) {
    return properties.getProperty(key);
  }

  private void load(String propFileName) {
    try {
      loadProperties(propFileName);
    } catch (IOException e) {
      LOGGER.info("Unable to load the property file due to {}", e.getMessage());
    }
  }
}
