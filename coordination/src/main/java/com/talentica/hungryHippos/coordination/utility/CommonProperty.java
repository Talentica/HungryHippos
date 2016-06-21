/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import java.io.IOException;
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

  public CommonProperty(String propFileName) {
    try {
      loadProperties(propFileName);
    } catch (IOException e) {
      LOGGER.info("Unable to load the property file due to {}", e.getMessage());
    }
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
}
