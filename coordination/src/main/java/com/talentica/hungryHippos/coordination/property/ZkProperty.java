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
