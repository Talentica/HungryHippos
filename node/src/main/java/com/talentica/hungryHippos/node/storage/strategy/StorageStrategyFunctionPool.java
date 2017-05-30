package com.talentica.hungryHippos.node.storage.strategy;

import com.talentica.hungryHippos.node.datareceiver.Calculator;
import com.talentica.hungryHippos.storage.StoreType;

/**
 * Created by rajkishoreh on 19/5/17.
 */
public enum StorageStrategyFunctionPool {
    INSTANCE;
    StorageStrategy keyWiseStorageStrategy;
    StorageStrategy indexWiseStorageStrategy;

    StorageStrategyFunctionPool() {
        keyWiseStorageStrategy = (buf, hhFileMapper, buckets, maxBucketSize) -> {
            String combinationKey = Calculator.combinationKeyGenerator(buckets);
            if (!hhFileMapper.storeRow(combinationKey, buf)) {
                hhFileMapper.sync();
                hhFileMapper.storeRow(combinationKey, buf);
            }
        };
        indexWiseStorageStrategy = (buf, hhFileMapper, buckets, maxBucketSize) -> {
            int index = Calculator.indexCalculator(buckets, maxBucketSize);
            hhFileMapper.storeRow(index, buf);
        };
    }

    public StorageStrategy getStore(StoreType storeType) {
        if (storeType == StoreType.INMEMORYDATASTORE || storeType == StoreType.HYBRIDDATASTORE)
            return keyWiseStorageStrategy;
        else
            return indexWiseStorageStrategy;
    }

}
