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
package com.talentica.hungryHippos.sharding;

/**
 * {@code ShardingTableFilesName} used for keeping the sharded file names.
 * 
 */
public enum ShardingTableFilesName {

  BUCKET_COMBINATION_TO_NODE_NUMBERS_MAP_FILE("bucketCombinationToNodeNumbersMap"),

  BUCKET_TO_NODE_NUMBER_MAP_FILE("bucketToNodeNumberMap"),

  KEY_TO_VALUE_TO_BUCKET_MAP_FILE("keyToValueToBucketMap");

  private String name;

  private ShardingTableFilesName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

}
