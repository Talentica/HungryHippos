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

import com.talentica.hungryHippos.node.service.HHFileMapper;

import java.io.IOException;

/**
 * Created by rajkishoreh on 18/5/17.
 */
@FunctionalInterface
public interface StorageStrategy {
    void store(byte[] buf, HHFileMapper hhFileMapper, int[] buckets, int maxBucketSize) throws IOException, InterruptedException;
}
