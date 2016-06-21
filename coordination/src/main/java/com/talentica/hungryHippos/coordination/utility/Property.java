/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import java.util.Properties;

/**
 * @author pooshans
 *
 */
public interface Property<T> {

  Properties getProperties();

  String getValueByKey(String key);

  default ClassLoader getClassLoader() {
    return this.getClass().getClassLoader();
  }

}
