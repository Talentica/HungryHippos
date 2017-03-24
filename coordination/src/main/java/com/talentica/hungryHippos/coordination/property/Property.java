/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
 * {@code Property<T>} is a generic class used for reading property files.
 * 
 * @author pooshans
 *
 */
public class Property<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Property.class.getName());
  private static Properties properties = null;
  private static ClassLoader loader = null;
  private static InputStream CONFIG_FILE_INPUT_STREAM;

  /**
   * create a new empty instance of Property.
   */
  public Property() {}

  /**
   * create a new instance of property with {@value propFileName}.
   * 
   * @param propFileName
   */
  public Property(String propFileName) {
    load(propFileName);
  }

  /**
   * create a Property<T> from the {@value propFileName}.
   * 
   * @param propFileName
   * @return Property<T>
   */
  public Property<T> setPropertyFile(String propFileName) {
    load(propFileName);
    return this;
  }

  /**
   * Ovverides existing Property<T> after changing the same instance on the basis of
   * {@value propFileNameWithPath}.
   * 
   * @param propFileNameWithPath
   * @return
   * @throws IOException Property<T>
   */
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

  /**
   * retrieves {@link Properties}.
   * 
   * @return {@link Properties}
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * retrieves the value of particular key in the {@link Properties}.
   * 
   * @param key
   * @return String, value associated with it.
   */
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

  private ClassLoader getClassLoader() {
    return this.getClass().getClassLoader();
  }
}
