package com.talentica.hungryHippos.sharding;

import java.util.HashMap;
import java.util.Map;

public class ShardingTableCache {

  private final Map<String, Object> shardingTableMap = new HashMap<>();

  ShardingTableZkService table = new ShardingTableZkService();

  public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";
  public final static String bucketCombinationToNodeNumbersMapFile =
      "bucketCombinationToNodeNumbersMap";
  public final static String keyToValueToBucketMapFile = "keyToValueToBucketMap";

  private ShardingTableCache() {}

  public static ShardingTableCache newInstance() {
    return new ShardingTableCache();
  }

  public Object getShardingTableFromCache(String key) throws IllegalArgumentException {
    Object value = shardingTableMap.get(key);
    if (value == null) {
      if (key.equalsIgnoreCase(bucketCombinationToNodeNumbersMapFile)) {
        value = table.readBucketCombinationToNodeNumbersMap();
        shardingTableMap.put(bucketCombinationToNodeNumbersMapFile, value);
      } else if (key.equalsIgnoreCase(bucketToNodeNumberMapFile)) {
        value = table.readBucketToNodeNumberMap();
        shardingTableMap.put(bucketToNodeNumberMapFile, value);
      } else if (key.equalsIgnoreCase(keyToValueToBucketMapFile)) {
        value = table.readKeyToValueToBucketMap();
        shardingTableMap.put(keyToValueToBucketMapFile, value);
      } else {
        throw new IllegalArgumentException();
      }
    }
    return value;
  }

}
