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
package com.talentica.hungryHippos.sharding;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * {@code ShardingTableCache} used for storing sharding table details in cache.
 *
 */
public class ShardingTableCache {

  private static ShardingTableCache shardingTableCache = null;
  private final Map<String, Object> shardingTableMap = new HashMap<>();

  private ShardingTableZkService table = new ShardingTableZkService();

  private ShardingTableCache() {}

  /**
   * create new instance of ShardingTableCache.
   * 
   * @return
   */
  public static ShardingTableCache newInstance() {
    if (shardingTableCache == null) {
      shardingTableCache = new ShardingTableCache();
    }
    return shardingTableCache;
  }

  /**
   * retrieves the value of a key from the sharding table cache.
   * 
   * @param key
   * @param path
   * @return
   * @throws IllegalArgumentException
   */
  public Object getShardingTableFromCache(String key, String path) throws IllegalArgumentException {
    Object value = shardingTableMap.get(key);
    if (value == null) {
      if (key.equalsIgnoreCase(
          ShardingTableFilesName.BUCKET_COMBINATION_TO_NODE_NUMBERS_MAP_FILE.getName())) {
        value = table.readBucketCombinationToNodeNumbersMap(path);
        shardingTableMap.put(
            ShardingTableFilesName.BUCKET_COMBINATION_TO_NODE_NUMBERS_MAP_FILE.getName(), value);
      } else if (key
          .equalsIgnoreCase(ShardingTableFilesName.BUCKET_TO_NODE_NUMBER_MAP_FILE.getName())) {
        value = table.readBucketToNodeNumberMap(path);
        shardingTableMap.put(ShardingTableFilesName.BUCKET_TO_NODE_NUMBER_MAP_FILE.getName(),
            value);
      } else if (key
          .equalsIgnoreCase(ShardingTableFilesName.KEY_TO_VALUE_TO_BUCKET_MAP_FILE.getName())) {
        value = table.readKeyToValueToBucketMap(path);
        shardingTableMap.put(ShardingTableFilesName.KEY_TO_VALUE_TO_BUCKET_MAP_FILE.getName(),
            value);
      } else {
        throw new IllegalArgumentException();
      }
    }
    return value;
  }

}
