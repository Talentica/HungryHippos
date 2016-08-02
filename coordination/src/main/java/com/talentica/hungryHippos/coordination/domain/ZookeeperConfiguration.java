/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class ZookeeperConfiguration {

  Map<String, String> pathMap;
  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperConfiguration.class);


  /**
   * @param pathMap
   * @param sessionTimeout
   */
  public ZookeeperConfiguration(Map<String, String> pathMap) {
    this.pathMap = pathMap;
  }

  public Map<String, String> getPathMap() {
    return pathMap;
  }

  public void setPathMap(Map<String, String> pathMap) {
    this.pathMap = pathMap;
  }


}
