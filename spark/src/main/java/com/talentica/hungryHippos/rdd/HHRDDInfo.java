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
    private Map<String,int[]> fileNameToNodeIdsCache;
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
        int jobPrimaryDimensionIdx = 0;
        int maxBucketSize = 0;
        int[] jobShardingDimensionsArray = new int[jobShardingDimensions.size()];
        int i = 0;
        for(String shardingDimensionKey:jobShardingDimensionsKey){
            int bucketSize = bucketToNodeNumberMap.get(shardingDimensionKey).size();
            noOfPartitions = noOfPartitions * bucketSize;
            if(bucketSize>maxBucketSize){
                primaryDimensionKey = shardingDimensionKey;
                maxBucketSize = bucketSize;
                jobPrimaryDimensionIdx = i;
            }
            jobShardingDimensionsArray[i] = jobShardingDimensions.get(i);
            i++;
        }

        int[][] combinationArray = new int[noOfPartitions][];
        populateCombination(combinationArray,null,0,jobShardingDimensionsArray,0);

        Partition[] partitions = new HHRDDPartition[noOfPartitions];
        for (int index = 0; index < noOfPartitions; index++) {
            List<Tuple2<String,int[]>> files = new ArrayList<>();
            listFile(files, "", 0, noOfShardingDimensions, jobShardingDimensionsArray, combinationArray[index]);
            int preferredNodeId = bucketToNodeNumberMap.get(primaryDimensionKey).get(new Bucket<>(combinationArray[index][jobPrimaryDimensionIdx])).getNodeId();
            String preferredHost = nodIdToIp.get(preferredNodeId);
            partitions[index] = new HHRDDPartition(id, index, new File(hipposRDDConf.getDirectoryLocation()).getPath(),
                    hipposRDDConf.getFieldTypeArrayDataDescription(),preferredHost,files,nodIdToIp);
        }
        return partitions;
    }

    private int populateCombination(int[][] combinationArray,String combination,int index,int[] jobShardingDimensions, int i ){
        if(i==jobShardingDimensions.length){
            String[] strings = combination.split("-");
            int[] intCombination = new int[strings.length];
            for (int j = 0; j <strings.length ; j++) {
                intCombination[j] = Integer.parseInt(strings[j]);
            }
            combinationArray[index]= intCombination;
            index++;
            return index;
        }

        for (int j = 0; j < bucketToNodeNumberMap.get(keyOrder[jobShardingDimensions[i]]).size(); j++) {
            String newCombination;
            if(i!=0){
                newCombination = combination+"-"+j;
            }else {
                newCombination = j+"";
            }
            index = populateCombination(combinationArray, newCombination,index, jobShardingDimensions, i+1);
        }

        return index;
    }



    private void listFile(List<Tuple2<String,int[]>> files, String fileName, int dim, int noOfShardingDimensions, int[] jobShardingDimensionsArray, int[] jobDimensionValues) {
        if (dim == noOfShardingDimensions) {
            Tuple2<String,int[]>  tuple2 = new Tuple2<>(fileName, getFileLocationNodeIds(fileName));
            files.add(tuple2);
            return;
        }
        boolean isJobShardingDimension =  false;
        int jobDimIdx = 0;
        for (int i = 0; i < jobShardingDimensionsArray.length; i++) {
            if(dim==jobShardingDimensionsArray[i]){
                isJobShardingDimension = true;
                break;
            }
            jobDimIdx++;
        }
        if (isJobShardingDimension) {
            if (dim == 0) {
                listFile(files, jobDimensionValues[jobDimIdx] + fileName, dim + 1, noOfShardingDimensions, jobShardingDimensionsArray, jobDimensionValues);
            } else {
                listFile(files, fileName + "_" + jobDimensionValues[jobDimIdx], dim + 1, noOfShardingDimensions, jobShardingDimensionsArray, jobDimensionValues);
            }
        } else {
            for (int i = 0; i < bucketToNodeNumberMap.get(keyOrder[dim]).size(); i++) {
                if (dim == 0) {
                    listFile(files, i + fileName, dim + 1, noOfShardingDimensions, jobShardingDimensionsArray, jobDimensionValues);
                } else {
                    listFile(files, fileName + "_" + i, dim + 1, noOfShardingDimensions, jobShardingDimensionsArray, jobDimensionValues);
                }
            }
        }
    }

    private int[] getFileLocationNodeIds(String fileName){
        int[] nodeIds = fileNameToNodeIdsCache.get(fileName);
        if(nodeIds==null) {
            nodeIds = new int[keyOrder.length];
            String[] buckets = fileName.split("_");
            Map<String, Bucket<KeyValueFrequency>> keyValueCombination = new HashMap<>();
            for (int i = 0; i < keyOrder.length; i++) {
                keyValueCombination.put(keyOrder[i], new Bucket<>(Integer.parseInt(buckets[i])));
            }
            BucketCombination bucketCombination = new BucketCombination(keyValueCombination);
            Set<Node> nodes = bucketCombinationToNodeNumberMap.get(bucketCombination);
            int i = 0 ;
            for (Node node : nodes) {
                nodeIds[i]= node.getNodeId();
                i++;
            }

            fileNameToNodeIdsCache.put(fileName,nodeIds);
        }
        return nodeIds;
    }


}
