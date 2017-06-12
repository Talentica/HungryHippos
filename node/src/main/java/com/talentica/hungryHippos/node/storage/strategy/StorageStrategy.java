package com.talentica.hungryHippos.node.storage.strategy;

import com.talentica.hungryHippos.node.service.HHFileMapper;

import java.io.IOException;

/**
 * Created by rajkishoreh on 18/5/17.
 */
@FunctionalInterface
public interface StorageStrategy {
    void store(byte[] buf, HHFileMapper hhFileMapper, int[] buckets, int maxBucketSize) throws IOException, InterruptedException;
}
