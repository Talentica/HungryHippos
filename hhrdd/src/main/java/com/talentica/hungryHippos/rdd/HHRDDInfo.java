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
 * The interface for interacting with {@link HHRDD} metadata information
 */
public interface HHRDDInfo extends Serializable {

  int[] getShardingIndexes();

  String[] getKeyOrder();

  Partition[] getOptimizedPartitions(int id, int noOfExecutors, List<Integer> jobShardingDimensions,
      int jobPrimaryDimensionIdx, List<String> jobShardingDimensionsKey,
      String primaryDimensionKey);

  Partition[] getPartitions(int id, int noOfExecutors, List<Integer> jobShardingDimensions,
      int jobPrimaryDimensionIdx, List<String> jobShardingDimensionsKey,
      String primaryDimensionKey);

  HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap();

  FieldTypeArrayDataDescription getFieldDataDesc();

}
