/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.coordination.utility;

/**
 * {@code ZkNodeName}
 * 
 * @author pooshans
 *
 */
public enum ZkNodeName {

  SHARDING_TABLE("sharding-table"),

  BUCKET_COMBINATION("bucket_combination"),

  KEY_TO_BUCKET_NUMBER("key_to_bucket_number"),

  KEY_TO_VALUE_TO_BUCKET("key_to_value_to_bucket"),

  KEY_TO_BUCKET("key_to_bucket"),

  NODES("nodes"),

  BUCKET("bucket"),

  ID("id"),

  UNDERSCORE("_"),

  NODE("node"),

  EQUAL("=");

  private String name;

  private ZkNodeName(String name) {
    this.name = name;
  }

  /**
   * retreives the name.
   * 
   * @return String
   */
  public String getName() {
    return name;
  }

}
