/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.util.Map;

/**
 * {@code ZookeeperConfiguration} holds all the details of the System related path.
 * 
 * @author PooshanS
 *
 */
public class ZookeeperConfiguration {

  Map<String, String> pathMap;

  /**
   * creates a new instance of ZookeeperConfiguration from ({@value pathMap}.
   * 
   * @param pathMap
   * @param sessionTimeout
   */
  public ZookeeperConfiguration(Map<String, String> pathMap) {
    this.pathMap = pathMap;
  }

  /**
   * Retrieves the Map associated with this instance.
   * 
   * @return a Map.
   */
  public Map<String, String> getPathMap() {
    return pathMap;
  }

  /**
   * Sets a Map instance to this instance.
   * 
   * @param pathMap.
   */
  public void setPathMap(Map<String, String> pathMap) {
    this.pathMap = pathMap;
  }


}
