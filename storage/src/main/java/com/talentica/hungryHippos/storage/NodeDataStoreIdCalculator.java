package com.talentica.hungryHippos.storage;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketsCalculator;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.utility.MapUtils;

/**
 * Created by debasishc on 26/8/15.
 */
public class NodeDataStoreIdCalculator implements Serializable {

  private static final long serialVersionUID = -4962284637100465382L;
  private Map<String, Set<Bucket<KeyValueFrequency>>> keyWiseAcceptingBuckets = new HashMap<>();
  private final String[] keys;
  private final DynamicMarshal dynamicMarshal;
  private static final Logger logger = LoggerFactory.getLogger(NodeDataStoreIdCalculator.class);
  private BucketsCalculator bucketsCalculator;
  private ShardingApplicationContext context;

  public NodeDataStoreIdCalculator(
      Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap,
      Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap, int thisNode,
      DataDescription dataDescription, ShardingApplicationContext context) {
    bucketsCalculator = new BucketsCalculator(keyToValueToBucketMap, context);
    keys = dataDescription.keyOrder();
    setKeyWiseAcceptingBuckets(bucketToNodeNumberMap, thisNode);
    this.dynamicMarshal = new DynamicMarshal(dataDescription);
    this.context = context;
  }

  private void setKeyWiseAcceptingBuckets(
      Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap, int thisNode) {
    logger.info("bucketToNodeNumberMap size {} on Node {}",bucketToNodeNumberMap.size(),thisNode);
    for (String key : bucketToNodeNumberMap.keySet()) {
      Set<Bucket<KeyValueFrequency>> keyWiseBuckets = new HashSet<>();
      Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = bucketToNodeNumberMap.get(key);
      logger.info("bucketToNodeMap size {} for key {}",bucketToNodeNumberMap.size(),key);
      for (Bucket<KeyValueFrequency> bucket : bucketToNodeMap.keySet()) {
        logger.info("Node id from bucketToNodeMap {}",bucketToNodeMap.get(bucket).getNodeId());
        if (bucketToNodeMap.get(bucket).getNodeId() == thisNode) {
          keyWiseBuckets.add(bucket);
        }
      }
      this.keyWiseAcceptingBuckets.put(key, keyWiseBuckets);
    }
    logger.info("keyWiseAcceptingBuckets:{}", MapUtils.getFormattedString(keyWiseAcceptingBuckets));
  }

  public int storeId(ByteBuffer row) {
    int fileId = 0;
    for (int i = keys.length - 1; i >= 0; i--) {
      fileId <<= 1;
      String key = keys[i];
      int keyIndex = Integer.parseInt(key.substring(3)) - 1;
      Object value = dynamicMarshal.readValue(keyIndex, row);
      Bucket<KeyValueFrequency> valueBucket = bucketsCalculator.getBucketNumberForValue(key, value);
      if (valueBucket != null && keyWiseAcceptingBuckets.get(keys[i]) != null
          && keyWiseAcceptingBuckets.get(keys[i]).contains(valueBucket)) {
        fileId |= 1;
      }
    }
    return fileId;
  }

}
