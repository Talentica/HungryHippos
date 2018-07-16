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
import java.util.Set;

public class NodeFileNamesIdentifier {

    private NodeSelector nodeSelector;

    private String[] keyOrder;

    private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;

    private int maxBucketSize;

    private Map<Integer, String> fileNames;

    public NodeFileNamesIdentifier(String[] keyOrder, HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap, int maxBucketSize) {
        this.nodeSelector = new NodeSelector();
        this.keyOrder = keyOrder;
        this.bucketToNodeNumberMap = bucketToNodeNumberMap;
        this.maxBucketSize = maxBucketSize;
        this.fileNames = new HashMap<>();
        addFileNameToList(fileNames, 0, "", 0, null);
    }

    private void addFileNameToList(Map<Integer, String> fileNames, int index, String fileName, int dimension, Map<String, Bucket<KeyValueFrequency>> keyBucket) {
        if (dimension == keyOrder.length) {
            addFileName(fileNames, index, fileName, keyBucket);
            return;
        }
        String key = keyOrder[dimension];
        Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
        for (Map.Entry<Bucket<KeyValueFrequency>, Node> bucketNodeEntry : bucketNodeMap.entrySet()) {

            if (dimension != 0) {
                keyBucket.put(key, bucketNodeEntry.getKey());
                int bucketId = bucketNodeEntry.getKey().getId();
                int newIndex = index + bucketId * (int) Math.pow(maxBucketSize, dimension);
                addFileNameToList(fileNames, newIndex, fileName + "_" + bucketId, dimension + 1, keyBucket);
            } else {
                keyBucket = new HashMap<>();
                keyBucket.put(key, bucketNodeEntry.getKey());
                int bucketId = bucketNodeEntry.getKey().getId();
                int newIndex = index + bucketId * (int) Math.pow(maxBucketSize, dimension);
                addFileNameToList(fileNames, newIndex, bucketId + fileName, dimension + 1, keyBucket);
            }
        }
    }

    private void addFileName(Map<Integer, String> fileNames, int index, String fileName, Map<String, Bucket<KeyValueFrequency>> keyBucket) {
        BucketCombination bucketCombination = new BucketCombination(keyBucket);
        Set<Integer> nodeIds = nodeSelector.selectNodeIds(bucketCombination, bucketToNodeNumberMap, keyOrder);
        if (nodeIds.contains(NodeInfo.INSTANCE.getIdentifier())) {
            fileNames.put(index, fileName);
        }
    }

    public Map<Integer, String> getFileNames() {
        return fileNames;
    }
}
