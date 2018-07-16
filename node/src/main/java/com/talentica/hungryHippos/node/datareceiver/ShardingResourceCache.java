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

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.DataDistributorStarter;
import com.talentica.hungryHippos.node.service.CacheClearService;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.util.Counter;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rajkishoreh on 11/7/17.
 */
public enum ShardingResourceCache {
    INSTANCE;
    private static Logger logger = LoggerFactory.getLogger(ShardingResourceCache.class);
    private Map<String, ShardingApplicationContext> contextMap = new HashMap<>();
    private Map<String, Counter> counterMap = new HashMap<>();
    private Map<String, HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>>> keyToBucketToNodeMapCache = new HashMap<>();
    private Map<String, HashMap<String, HashMap<String, List<Bucket<KeyValueFrequency>>>>> keyToValueToBucketMapCache = new HashMap<>();
    private Map<String, HashMap<String, HashMap<String, Integer>>> splittedKeyValueMapCache = new HashMap<>();
    private Map<String, Map<Integer, String>> fileNamesCache = new HashMap<>();
    private Map<String, HHFileNamesIdentifier> hhfileNamesCalculatorCache = new HashMap<>();
    private Map<String, Map<Integer, Map<Integer, String>>> interMediateFileNamesCache = new HashMap<>();
    private Map<String, Integer> reduceFactorCache = new HashMap<>();
    private Map<String, Integer> maxFilesCache = new HashMap<>();

    public synchronized ShardingApplicationContext getContext(String hhFilePath) throws JAXBException, FileNotFoundException {
        ShardingApplicationContext context = contextMap.get(hhFilePath);
        Counter counter = counterMap.get(hhFilePath);
        if (context == null) {
            logger.info("Initializing context for {}",hhFilePath);
            context = new ShardingApplicationContext(getShardingTableLocation(hhFilePath));
            FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
            dataDescription.setKeyOrder(context.getShardingDimensions());
            contextMap.put(hhFilePath, context);
            counter = new Counter(0);
            counterMap.put(hhFilePath, counter);

            String keyToBucketToNodePath = context.getBuckettoNodeNumberMapFilePath();
            HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> keyToBucketToNodeMap =
                    ShardingFileUtil.readFromFileBucketToNodeNumber(keyToBucketToNodePath);
            keyToBucketToNodeMapCache.put(hhFilePath, keyToBucketToNodeMap);

            String keyToValueToBucketPath = context.getKeytovaluetobucketMapFilePath();
            Map<String, String> dataTypeMap = ShardingFileUtil.getDataTypeMap(context);
            HashMap<String, HashMap<String, List<Bucket<KeyValueFrequency>>>> keyToValueToBucketMap =
                    ShardingFileUtil.readFromFileKeyToValueToBucket(keyToValueToBucketPath, dataTypeMap);
            keyToValueToBucketMapCache.put(hhFilePath, keyToValueToBucketMap);

            String splittedKeyValuePath = context.getSplittedKeyValueMapFilePath();
            HashMap<String, HashMap<String, Integer>> splittedKeyValueMap =
                    ShardingFileUtil.readFromFileSplittedKeyValue(splittedKeyValuePath);
            splittedKeyValueMapCache.put(hhFilePath, splittedKeyValueMap);

            int maxBucketSize = Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize());
            String[] keyOrder = context.getShardingDimensions();
            int maxFiles = (int) Math.pow(maxBucketSize, keyOrder.length);
            maxFilesCache.put(hhFilePath, maxFiles);

            int reduceFactor = 1;
            while (reduceFactor * reduceFactor < maxFiles) {
                reduceFactor++;
            }
            reduceFactorCache.put(hhFilePath, reduceFactor);
        }
        counter.incrementAndGet();
        return context;
    }

    public void releaseContext(String hhFilePath){
        releaseCacheContext(hhFilePath);
        System.gc();
    }

    private synchronized void releaseCacheContext(String hhFilePath) {
        Counter counter = counterMap.get(hhFilePath);
        if (counter != null) {
            if (counter.decrementAndGet() <= 0) {
                logger.info("Destroying context for {}",hhFilePath);
                contextMap.remove(hhFilePath);
                counterMap.remove(hhFilePath);
                keyToBucketToNodeMapCache.remove(hhFilePath);
                keyToValueToBucketMapCache.remove(hhFilePath);
                splittedKeyValueMapCache.remove(hhFilePath);
                fileNamesCache.remove(hhFilePath);
                interMediateFileNamesCache.remove(hhFilePath);
                maxFilesCache.remove(hhFilePath);
                reduceFactorCache.remove(hhFilePath);
                hhfileNamesCalculatorCache.remove(hhFilePath);
                DataDistributorStarter.cacheClearServices.execute(new CacheClearService());
            }
        }
    }

    public int getMaxFiles(String hhFilePath){
        return maxFilesCache.get(hhFilePath);
    }

    public int getReduceFactor(String hhFilePath){
        return reduceFactorCache.get(hhFilePath);
    }

    public HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> getKeyToBucketToNodeMap(String hhFilePath) {
        return keyToBucketToNodeMapCache.get(hhFilePath);
    }

    public HashMap<String, HashMap<String, List<Bucket<KeyValueFrequency>>>> getKeyToValueToBucketMap(String hhFilePath) {
        return keyToValueToBucketMapCache.get(hhFilePath);
    }

    public HashMap<String, HashMap<String, Integer>> getSplittedKeyValueMap(String hhFilePath) {
        return splittedKeyValueMapCache.get(hhFilePath);
    }

    public HHFileNamesIdentifier getHHFileNamesCalculator(String hhFilePath) {
        HHFileNamesIdentifier hhFileNamesIdentifier = hhfileNamesCalculatorCache.get(hhFilePath);
        if (hhFileNamesIdentifier == null) {
            return generateHHFileNamesCalculator(hhFilePath);
        }
        return hhFileNamesIdentifier;
    }

    private synchronized HHFileNamesIdentifier generateHHFileNamesCalculator(String hhFilePath) {
        HHFileNamesIdentifier hhFileNamesIdentifier = hhfileNamesCalculatorCache.get(hhFilePath);
        if (hhFileNamesIdentifier == null) {
            ShardingApplicationContext context = contextMap.get(hhFilePath);
            hhFileNamesIdentifier = new HHFileNamesIdentifier(context.getShardingDimensions(),
                    keyToBucketToNodeMapCache.get(hhFilePath), Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize()));
            hhfileNamesCalculatorCache.put(hhFilePath, hhFileNamesIdentifier);
        }
        return hhFileNamesIdentifier;
    }

    public Map<Integer, String> getFileNames(String hhFilePath) {
        Map<Integer, String> fileNames = fileNamesCache.get(hhFilePath);
        if (fileNames == null) {
            return generateFileNames(hhFilePath);
        }
        return fileNames;
    }

    private synchronized Map<Integer, String> generateFileNames(String hhFilePath) {
        Map<Integer, String> fileNames = fileNamesCache.get(hhFilePath);
        if (fileNames == null) {
            ShardingApplicationContext context = contextMap.get(hhFilePath);
            NodeFileNamesIdentifier nodeFileNamesIdentifier = new NodeFileNamesIdentifier(context.getShardingDimensions(),
                    keyToBucketToNodeMapCache.get(hhFilePath), Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize()));
            fileNames = nodeFileNamesIdentifier.getFileNames();
            fileNamesCache.put(hhFilePath, fileNames);
        }
        return fileNames;
    }

    public Map<Integer, String> getFileNames(String hhFilePath, int fileId) {
        Map<Integer, Map<Integer, String>> intemediateFilesMap = interMediateFileNamesCache.get(hhFilePath);
        if (intemediateFilesMap == null || intemediateFilesMap.get(fileId) == null) {
            return generateFileNames(hhFilePath, fileId);
        }
        return intemediateFilesMap.get(fileId);
    }


    private synchronized Map<Integer, String> generateFileNames(String hhFilePath, int fileId) {
        Map<Integer, Map<Integer, String>> interMediateFilesMap = interMediateFileNamesCache.get(hhFilePath);
        if (interMediateFilesMap == null) {
            interMediateFilesMap = new HashMap<>();
            interMediateFileNamesCache.put(hhFilePath, interMediateFilesMap);
        }
        Map<Integer, String> fileNames = interMediateFilesMap.get(fileId);
        if (fileNames == null) {
            ShardingApplicationContext context = contextMap.get(hhFilePath);
            SecondStageNodeFileNamesIdentifier nodeFileNamesCalculator = new SecondStageNodeFileNamesIdentifier(context.getShardingDimensions(),
                    keyToBucketToNodeMapCache.get(hhFilePath), Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize())
                    , fileId, reduceFactorCache.get(hhFilePath));
            fileNames = nodeFileNamesCalculator.getFileNames();
            interMediateFilesMap.put(fileId, fileNames);
        }
        return fileNames;
    }

    /**
     * Returns Sharding Table Location
     *
     * @return
     */
    private String getShardingTableLocation(String hhFilePath) {
        String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
        String localDir = fileSystemBaseDirectory + hhFilePath;
        String shardingTableLocation =
                localDir + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        return shardingTableLocation;
    }


}

