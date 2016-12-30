package com.talentica.hungryHippos.rdd;

import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import org.apache.spark.Partition;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Created by rajkishoreh on 30/12/16.
 */
public class HHRDDInfo implements Serializable {
    public Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap;
    private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;
    private String[]  keyOrder;
    private Map<String,Set<String>> fileNameToNodeIdsCache;
    private Map<Integer,String> nodIdToIp;

    public HHRDDInfo(Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumberMap, HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap, String[] keyOrder,Map<Integer,String> nodIdToIp) {
        this.bucketCombinationToNodeNumberMap = bucketCombinationToNodeNumberMap;
        this.bucketToNodeNumberMap = bucketToNodeNumberMap;
        this.keyOrder = keyOrder;
        this.nodIdToIp = nodIdToIp;
        this.fileNameToNodeIdsCache= new HashMap<>();
    }

    public Map<BucketCombination, Set<Node>> getBucketCombinationToNodeNumberMap() {
        return bucketCombinationToNodeNumberMap;
    }

    public HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
        return bucketToNodeNumberMap;
    }

    public String[] getKeyOrder() {
        return keyOrder;
    }

    public Partition[] getPartition(HHRDDConfigSerialized hipposRDDConf, int id, List<Integer> jobShardingDimensions, List<String> jobShardingDimensionsKey) {
        int noOfShardingDimensions = hipposRDDConf.getShardingKeyOrder().length;
        int noOfPartitions = 1;
        String primaryDimensionKey = null;
        int maxBucketSize = 0;
        for(String shardingDimensionKey:jobShardingDimensionsKey){
            int bucketSize = bucketToNodeNumberMap.get(shardingDimensionKey).size();
            noOfPartitions = noOfPartitions * bucketSize;
            if(bucketSize>maxBucketSize){
                primaryDimensionKey = shardingDimensionKey;
                maxBucketSize = bucketSize;
            }

        }
        Partition[] partitions = new HHRDDPartition[noOfPartitions];
        for (int index = 0; index < noOfPartitions; index++) {
            List<Tuple2<String,Set<String>>> files = new ArrayList<>();
            listFile(files, "", 0, noOfShardingDimensions, jobShardingDimensions, index);
            int preferredNodeId = bucketToNodeNumberMap.get(primaryDimensionKey).get(new Bucket<>(index)).getNodeId();
            String preferredHost = nodIdToIp.get(preferredNodeId);
            partitions[index] = new HHRDDPartition(id, index, new File(hipposRDDConf.getDirectoryLocation()).getPath(),
                    hipposRDDConf.getFieldTypeArrayDataDescription(),preferredHost,files);
        }
        return partitions;
    }

    private void listFile(List<Tuple2<String,Set<String>>> files, String fileName, int dim, int noOfShardingDimensions, List<Integer> jobShardingDimensions, int primDimValue) {
        if (dim == noOfShardingDimensions) {
            Tuple2<String,Set<String>>  tuple2 = new Tuple2<>(fileName, getFileLocationNodeIds(fileName));
            files.add(tuple2);
            return;
        }
        if (jobShardingDimensions.contains(dim)) {
            if (dim == 0) {
                listFile(files, primDimValue + fileName, dim + 1, noOfShardingDimensions, jobShardingDimensions, primDimValue);
            } else {
                listFile(files, fileName + "_" + primDimValue, dim + 1, noOfShardingDimensions, jobShardingDimensions, primDimValue);
            }
        } else {
            for (int i = 0; i < bucketToNodeNumberMap.get(keyOrder[dim]).size(); i++) {
                if (dim == 0) {
                    listFile(files, i + fileName, dim + 1, noOfShardingDimensions, jobShardingDimensions, primDimValue);
                } else {
                    listFile(files, fileName + "_" + i, dim + 1, noOfShardingDimensions, jobShardingDimensions, primDimValue);
                }
            }
        }
    }

    private Set<String> getFileLocationNodeIds(String fileName){
        Set<String> nodeIps = fileNameToNodeIdsCache.get(fileName);
        if(nodeIps==null) {
            String[] buckets = fileName.split("_");
            Map<String, Bucket<KeyValueFrequency>> keyValueCombination = new HashMap<>();
            for (int i = 0; i < keyOrder.length; i++) {
                keyValueCombination.put(keyOrder[i], new Bucket<>(Integer.parseInt(buckets[i])));
            }
            BucketCombination bucketCombination = new BucketCombination(keyValueCombination);
            Set<Node> nodes = bucketCombinationToNodeNumberMap.get(bucketCombination);
            for (Node node : nodes) {
                nodeIps.add(nodIdToIp.get(node.getNodeId()));
            }

            fileNameToNodeIdsCache.put(fileName,nodeIps);
        }
        return nodeIps;
    }


}
