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
