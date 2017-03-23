/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.FileJoiner;
import com.talentica.hungryHippos.node.datareceiver.HHFileUploader;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by rajkishoreh on 6/2/17.
 */
public class HHFileMapper {

    private String[] keyOrder;
    private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;
    private Map<BucketCombination, Set<Node>> bucketCombinationNodeMap;
    private int maxBucketSize;
    private Map<Integer, Set<String>> nodeToFileMap;
    private DataStore dataStore;
    private String hhFilePath;
    private String uniqueFolderName;

    public HHFileMapper(String hhFilePath, ShardingApplicationContext context, DataDescription dataDescription
            , HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap, Map<BucketCombination, Set<Node>> bucketCombinationNodeMap, String[] keyOrder) throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
        this.hhFilePath = hhFilePath;
        this.bucketCombinationNodeMap = bucketCombinationNodeMap;
        this.bucketToNodeNumberMap = bucketToNodeNumberMap;
        maxBucketSize = Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize());
        Map<Integer, String> fileNames = new HashMap<>();
        nodeToFileMap = new HashMap<>();
        this.keyOrder = keyOrder;
        addFileNameToList(fileNames, 0, "", 0, null);
        this.uniqueFolderName = UUID.randomUUID().toString();
        dataStore = new FileDataStore(fileNames, maxBucketSize, keyOrder.length,
                 hhFilePath, uniqueFolderName);
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
        Set<Node> nodes = bucketCombinationNodeMap.get(bucketCombination);
        Iterator<Node> nodeIterator = nodes.iterator();
        while (nodeIterator.hasNext()) {
            Node node = nodeIterator.next();
            int nodeId = node.getNodeId();
            Set<String> fileNameSet = nodeToFileMap.get(nodeId);
            if (fileNameSet == null) {
                fileNameSet = new HashSet<>();
                nodeToFileMap.put(nodeId, fileNameSet);
            }
            fileNameSet.add(fileName);
        }
        fileNames.put(index, fileName);
    }

    public void storeRow(int index, byte[] raw) {
        dataStore.storeRow(index,raw);
    }

    public void sync() throws IOException, InterruptedException {
        dataStore.sync();
        String baseFolderPath = FileSystemContext.getRootDirectory() + hhFilePath;
        String srcFolderPath = baseFolderPath + File.separator + uniqueFolderName;
        String destFolderPath = baseFolderPath + File.separator + FileSystemContext.getDataFilePrefix();
        //FileJoiner.INSTANCE.join(srcFolderPath, destFolderPath, destFolderPath);
        HHFileUploader.INSTANCE.uploadFile(srcFolderPath, destFolderPath, nodeToFileMap, hhFilePath);
        FileUtils.deleteDirectory(new File(srcFolderPath));
    }
}
