package com.talentica.hungryHippos.sharding;

import java.util.Map;

/**
 * Created by AmitG on 4/12/2016.
 */
public class BucketUtil {
    public static Bucket getBucket(Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap, int bucketNo) {
        Bucket<KeyValueFrequency> bucket = null;
        if (valueToBucketMap != null) {
            for (Bucket<KeyValueFrequency> tempBucket : valueToBucketMap.values()) {
                if (tempBucket.getId() == bucketNo) {
                    bucket = tempBucket;
                    break;
                }
            }
        }
        return bucket;
    }
}
