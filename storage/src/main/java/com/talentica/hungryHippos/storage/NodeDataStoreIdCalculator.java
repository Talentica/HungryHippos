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

  public NodeDataStoreIdCalculator(
      HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap, int thisNode,
      DataDescription dataDescription, ShardingApplicationContext context) {
    bucketsCalculator = new BucketsCalculator(keyToValueToBucketMap, context);
    keys = dataDescription.keyOrder();
    setKeyWiseAcceptingBuckets(bucketToNodeNumberMap, thisNode);
    this.dynamicMarshal = new DynamicMarshal(dataDescription);
  }

  private void setKeyWiseAcceptingBuckets(
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap, int thisNode) {
    for (String key : bucketToNodeNumberMap.keySet()) {
      Set<Bucket<KeyValueFrequency>> keyWiseBuckets = new HashSet<>();
      HashMap<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = bucketToNodeNumberMap.get(key);
      for (Bucket<KeyValueFrequency> bucket : bucketToNodeMap.keySet()) {
        if (bucketToNodeMap.get(bucket).getNodeId() == thisNode) {
          keyWiseBuckets.add(bucket);
        }
      }
      this.keyWiseAcceptingBuckets.put(key, keyWiseBuckets);
    }
    logger.info("keyWiseAcceptingBuckets:{}", MapUtils.getFormattedString(keyWiseAcceptingBuckets));
  }

  public String storeId(ByteBuffer row) {
    StringBuilder fileName = new StringBuilder();
    for (int i = 0; i < keys.length; i++) {
      String key = keys[i];
      int keyIndex = Integer.parseInt(key.substring(3)) - 1;
      Object value = dynamicMarshal.readValue(keyIndex, row);
      Bucket<KeyValueFrequency> valueBucket = bucketsCalculator.getBucketNumberForValue(key, value);
      if(i!=0){
        fileName.append("_");
      }
      fileName.append(valueBucket.getId());
    }
    return fileName.toString();
  }

}
