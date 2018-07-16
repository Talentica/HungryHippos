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
