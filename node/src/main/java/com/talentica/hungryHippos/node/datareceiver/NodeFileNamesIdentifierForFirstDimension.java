/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.util.NodeSelector;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public class NodeFileNamesIdentifierForFirstDimension {

    private String[] keyOrder;

    private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;

    private Map<Integer, String> indexToFileNames;

    private Map<Integer, Integer> multiplicationFactor;

    public NodeFileNamesIdentifierForFirstDimension(String[] keyOrder, HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap, int maxBucketSize) {
        this.keyOrder = keyOrder;
        this.bucketToNodeNumberMap = bucketToNodeNumberMap;
        this.indexToFileNames = new HashMap<>();
        this.multiplicationFactor = new HashMap<>();
        this.multiplicationFactor.put(1,maxBucketSize);
        for (int dim = 2; dim < keyOrder.length; dim++) {
            this.multiplicationFactor.put(dim, (int) Math.pow(maxBucketSize, dim));
        }
        addFileNameToList();
    }

    private void addFileNameToList() {
        String key = keyOrder[0];
        Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
        for (Map.Entry<Bucket<KeyValueFrequency>, Node> bucketNodeEntry : bucketNodeMap.entrySet()) {
            Map<String, Bucket<KeyValueFrequency>> keyBucket = new HashMap<>();
            keyBucket.put(key, bucketNodeEntry.getKey());
            int bucketId = bucketNodeEntry.getKey().getId();
            int newIndex = bucketId;
            addFileNameToListUtil(newIndex, bucketId + "",  1, keyBucket);
        }
    }

    private void addFileNameToListUtil(int index, String fileName, int dimension, Map<String, Bucket<KeyValueFrequency>> keyBucket) {
        if (dimension == keyOrder.length) {
            addFileName(index, fileName, keyBucket);
            return;
        }
        String key = keyOrder[dimension];
        if (keyBucket.get(key) != null) {
            int bucketId = keyBucket.get(key).getId();
            int newIndex = index;
            addFileNameToListUtil(newIndex, fileName + "_" + bucketId, dimension + 1, keyBucket);
        }else{
            Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
            for (Map.Entry<Bucket<KeyValueFrequency>, Node> bucketNodeEntry : bucketNodeMap.entrySet()) {
                keyBucket.put(key, bucketNodeEntry.getKey());
                int bucketId = bucketNodeEntry.getKey().getId();
                int newIndex = index + bucketId * multiplicationFactor.get(dimension);
                addFileNameToListUtil(newIndex, fileName + "_" + bucketId, dimension + 1, keyBucket);
            }
            keyBucket.remove(key);
        }
    }


    private void addFileName(int index, String fileName, Map<String, Bucket<KeyValueFrequency>> keyBucket) {
        BucketCombination bucketCombination = new BucketCombination(keyBucket);
        Node node = bucketToNodeNumberMap.get(keyOrder[0]).get(bucketCombination.getBucketsCombination().get(keyOrder[0]));
        if (node.getNodeId() == NodeInfo.INSTANCE.getIdentifier()) {
            indexToFileNames.put(index, fileName);
        }
    }

    public Map<Integer, String> getIndexToFileNames() {
        return indexToFileNames;
    }
}
