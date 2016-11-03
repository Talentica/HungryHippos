package com.talentica.torrent.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Environment } initializes torrent relates property.
 *
 */
public final class Environment {

  private static final Logger LOGGER = LoggerFactory.getLogger(Environment.class);

  static final String PROPERTY_NAME_FILE_PATH_OVERRIDE = "torrent.properties.file.path";

  private static final String DEFAULT_PROPERTIES_FILE_NAME = "application.properties";

  private static Properties properties = new Properties();

  static {
    initialize();
  }

  private static void initialize() {
    String propertyFilePath = null;
    try {
      propertyFilePath = loadDefaultProperties();
      propertyFilePath = overrideDefaultsIfAny();
    } catch (IOException exception) {
      LOGGER.error("Error occurred while loading properties file from {}", propertyFilePath);
      LOGGER.error(exception.getMessage(), exception);
      throw new RuntimeException(exception);
    }
  }

  private static String loadDefaultProperties() throws IOException, FileNotFoundException {
    String propertiesFilePath =
        Environment.class.getClassLoader().getResource(DEFAULT_PROPERTIES_FILE_NAME).getPath();
    properties
        .load(Environment.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTIES_FILE_NAME));
    return propertiesFilePath;
  }

  private static String overrideDefaultsIfAny() throws IOException, FileNotFoundException {
    String propertyFilePath = System.getProperty(PROPERTY_NAME_FILE_PATH_OVERRIDE);
    if (propertyFilePath != null) {
      properties.load(new FileReader(new File(propertyFilePath)));
    }
    return propertyFilePath;
  }

  /**
   * 
   * @param key
   * @return String, which is the value for a particular key.
   */
  public static String getPropertyValue(String key) {
    return properties.getProperty(key);
  }

  /**
   * 
   * @param key
   * @return
   */
  public static int getPropertyValueAsInteger(String key) {
    return Integer.parseInt(properties.getProperty(key));
  }

  /**
   * retrieves the value of server base sleep time.
   * 
   * @return int, representing time in milliseconds.
   */
  public static int getCoordinationServerConnectionRetryBaseSleepTimeInMs() {
    return getPropertyValueAsInteger(
        "coordination.server.connection.retry.base.sleep.time.milliseconds");
  }

  /**
   * retrieves coordination server maximum retries allowed.
   * 
   * @return int, max times.
   */
  public static int getCoordinationServerConnectionRetryMaxTimes() {
    return getPropertyValueAsInteger("coordination.server.connection.retry.max.times");
  }

  /**
   * reinitialize the enviroment.
   */
  public static synchronized void reload() {
    initialize();
  }

}
