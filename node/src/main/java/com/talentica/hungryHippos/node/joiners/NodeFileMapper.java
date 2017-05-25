package com.talentica.hungryHippos.node.joiners;

import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.NodeSelector;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by rajkishoreh on 18/5/17.
 */
class NodeFileMapper {

    private String[] keyOrder;
    private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;
    private int maxBucketSize;
    private FileDataStore fileDataStore;
    private String uniqueFolderName;
    private NodeSelector nodeSelector;
    private Map<Integer, String> fileNames;

    public NodeFileMapper(String hhFilePath, ShardingApplicationContext context,
                          HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
                          String[] keyOrder) throws InterruptedException, ClassNotFoundException,
            JAXBException, KeeperException, IOException {
        this.bucketToNodeNumberMap = bucketToNodeNumberMap;
        maxBucketSize = Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize());
        fileNames = new HashMap<>();
        this.keyOrder = keyOrder;
        nodeSelector = new NodeSelector();
        addFileNameToList(fileNames, 0, "", 0, null);
        this.uniqueFolderName = FileSystemContext.getDataFilePrefix();
        this.fileDataStore = new FileDataStore(fileNames, maxBucketSize, keyOrder.length,
                hhFilePath, true, uniqueFolderName);
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

    public void storeRow(int index, byte[] raw, int off, int len) {
        fileDataStore.storeRow(index, raw, off, len);
    }

    public void sync() throws IOException, InterruptedException {
        fileDataStore.sync();
    }

    public boolean isUsingBufferStream() {
        return fileDataStore.isUsingBufferStream();
    }

    public int getNoOfFiles() {
        return fileNames.size();
    }

    public void upgradeStore() throws IOException {
        this.fileDataStore.upgradeStreams();
    }

    public Map<String, FileOutputStream> getFileNameToOutputStreamMap() {
        return this.fileDataStore.getFileNameToOutputStreamMap();
    }

    public Map<String, BufferedOutputStream> getFileNameToBufferedOutputStreamMap() {
        return this.fileDataStore.getFileNameToBufferedOutputStreamMap();
    }
}
