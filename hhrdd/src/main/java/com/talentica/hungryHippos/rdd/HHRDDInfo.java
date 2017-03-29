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
package com.talentica.hungryHippos.rdd;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.Partition;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;

/**
 * The interface for interacting with {@link HHRDD} metadata information.
 */
public interface HHRDDInfo extends Serializable {

  /**
   * Gets the sharding indexes.
   *
   * @return the sharding indexes
   */
  int[] getShardingIndexes();

  /**
   * Gets the key order.
   *
   * @return the key order
   */
  String[] getKeyOrder();

  /**
   * Gets the optimized partitions.
   *
   * @param id the id
   * @param noOfExecutors the no of executors
   * @param jobShardingDimensions the job sharding dimensions
   * @param jobPrimaryDimensionIdx the job primary dimension idx
   * @param jobShardingDimensionsKey the job sharding dimensions key
   * @param primaryDimensionKey the primary dimension key
   * @return the optimized partitions
   */
  Partition[] getOptimizedPartitions(int id, int noOfExecutors, List<Integer> jobShardingDimensions,
      int jobPrimaryDimensionIdx, List<String> jobShardingDimensionsKey,
      String primaryDimensionKey);

  /**
   * Gets the partitions.
   *
   * @param id the id
   * @param noOfExecutors the no of executors
   * @param jobShardingDimensions the job sharding dimensions
   * @param jobPrimaryDimensionIdx the job primary dimension idx
   * @param jobShardingDimensionsKey the job sharding dimensions key
   * @param primaryDimensionKey the primary dimension key
   * @return the partitions
   */
  Partition[] getPartitions(int id, int noOfExecutors, List<Integer> jobShardingDimensions,
      int jobPrimaryDimensionIdx, List<String> jobShardingDimensionsKey,
      String primaryDimensionKey);

  /**
   * Gets the bucket to node number map.
   *
   * @return the bucket to node number map
   */
  HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap();

  /**
   * Gets the field data desc.
   *
   * @return the field data desc
   */
  FieldTypeArrayDataDescription getFieldDataDesc();

}
