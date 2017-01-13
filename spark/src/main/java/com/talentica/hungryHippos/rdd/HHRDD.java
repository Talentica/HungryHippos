package com.talentica.hungryHippos.rdd;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author pooshans
 */
public class HHRDD extends RDD<byte[]> implements Serializable {
    private static final long serialVersionUID = 4074885953480955556L;
    private static final ClassTag<byte[]> HHRD_READER__TAG =
            ClassManifestFactory$.MODULE$.fromClass(byte[].class);
    private int id;
    private Partition[] partitions;
    private HHRDDInfo hhrddInfo;

    public HHRDD(JavaSparkContext sc, HHRDDInfo hhrddInfo, Integer[] jobDimensions, boolean requiresShuffle) {
        super(sc.sc(), new ArrayBuffer<Dependency<?>>(), HHRD_READER__TAG);

        this.hhrddInfo = hhrddInfo;
        this.id = sc.sc().newRddId();

        String[] keyOrder = hhrddInfo.getKeyOrder();
        int[] shardingIndexes = hhrddInfo.getShardingIndexes();
        List<Integer> jobShardingDimensions = new ArrayList<>();
        List<String> jobShardingDimensionsKey = new ArrayList<>();


        String primaryDimensionKey = null;
        int jobPrimaryDimensionIdx = 0;
        int maxBucketSize = 0;
        int jobDimensionIdx = 0;

        for (int i = 0; i < shardingIndexes.length; i++) {
            for (int j = 0; j < jobDimensions.length; j++) {
                if (shardingIndexes[i] == jobDimensions[j]) {
                    int bucketSize = hhrddInfo.getBucketToNodeNumberMap().get(keyOrder[i]).size();
                    String dimensionKey = keyOrder[i];
                    jobShardingDimensions.add(shardingIndexes[i]);
                    jobShardingDimensionsKey.add(keyOrder[i]);
                    if (bucketSize > maxBucketSize) {
                        primaryDimensionKey = dimensionKey;
                        jobPrimaryDimensionIdx = jobDimensionIdx;
                    }
                    jobDimensionIdx++;
                }
            }
        }
        if (jobShardingDimensions.isEmpty()) {
            jobPrimaryDimensionIdx = shardingIndexes[0];
            primaryDimensionKey = keyOrder[0];
            jobShardingDimensions.add(jobPrimaryDimensionIdx);
            jobShardingDimensionsKey.add(primaryDimensionKey);
            jobDimensionIdx++;
        }

        if (requiresShuffle) {
            int noOfExecutors = sc.defaultParallelism();
            this.partitions = hhrddInfo.getOptimizedPartitions(id, noOfExecutors, jobShardingDimensions,jobPrimaryDimensionIdx, jobShardingDimensionsKey,primaryDimensionKey);
        } else {
            this.partitions = hhrddInfo.getPartition(id, jobShardingDimensions, jobPrimaryDimensionIdx, jobShardingDimensionsKey, primaryDimensionKey);

        }

    }

    @Override
    public Iterator<byte[]> compute(Partition partition, TaskContext taskContext) {
        HHRDDPartition hhRDDPartion = (HHRDDPartition) partition;
        HHRDDIterator iterator = null;
        try {
            iterator = new HHRDDIterator(hhRDDPartion.getFilePath(), hhRDDPartion.getRowSize(), hhRDDPartion.getFiles(), hhRDDPartion.getNodIdToIp());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return iterator;
    }

    @Override
    public Partition[] getPartitions() {
        return this.partitions;
    }

    @Override
    public Seq<String> getPreferredLocations(Partition partition) {
        List<String> nodes = new ArrayList<>();
        nodes.addAll(((HHRDDPartition) partition).getPreferredHosts());
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }
        return scala.collection.JavaConversions.asScalaBuffer(nodes).seq();
    }
}
