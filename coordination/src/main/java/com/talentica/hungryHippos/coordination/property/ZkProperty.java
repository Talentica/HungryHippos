/**
 * 
 */
package com.talentica.hungryHippos.coordination.property;

/**
 * {@code ZkProperty} used for loading zookeeper related properties from a file.
 * 
 * @author pooshans
 *
 */
public class ZkProperty extends Property<ZkProperty> {
  /**
   * creates a new instance of ZkProperty.
   * 
   * @param propFileName
   */
  public ZkProperty(String propFileName) {
    super(propFileName);
  }


}
