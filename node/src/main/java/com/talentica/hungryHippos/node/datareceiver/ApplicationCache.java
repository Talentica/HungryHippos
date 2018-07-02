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
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.joiners.orc.OrcSchemaCreator;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.sharding.Column;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.filesystem.FileStatistics;
import com.talentica.hungryHippos.node.service.CacheClearService;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.util.Counter;
import com.talentica.hungryhippos.filesystem.SerializableComparator;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.util.*;

/**
 * Created by rajkishoreh on 11/7/17.
 */
public enum ApplicationCache {
    INSTANCE;
    private static Logger logger = LoggerFactory.getLogger(ApplicationCache.class);
    private Map<String, ShardingApplicationContext> contextMap = new HashMap<>();
    private Map<String, Counter> counterMap = new HashMap<>();
    private Map<String, HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>>> keyToBucketToNodeMapCache = new HashMap<>();
    private Map<String, HashMap<String, HashMap<String, List<Bucket<KeyValueFrequency>>>>> keyToValueToBucketMapCache = new HashMap<>();
    private Map<String, HashMap<String, HashMap<String, Integer>>> splittedKeyValueMapCache = new HashMap<>();
    private Map<String, Map<Integer, String>> indexToFileNamesCache = new HashMap<>();
    private Map<String, Map<Integer, String>> indexToFileNamesCacheForFirstDimension = new HashMap<>();
    private Map<String, Map<String,int[]>> hhfileToFileToNodeIdCache = new HashMap<>();
    private Map<String, HHFileNamesIdentifierForFirstDimension> hhfileNamesCalculatorCacheForFirstDimension = new HashMap<>();
    private Map<String, Map<Integer, Map<Integer, String>>> interMediateFileNamesCache = new HashMap<>();
    private Map<String, Integer> reduceFactorCache = new HashMap<>();
    private Map<String, Integer> maxFilesCache = new HashMap<>();
    private Map<String, Map<String,FileStatistics>> fileStatisticsMapCache = new HashMap<>();
    private Map<String, FileStatistics[]> fileStatisticsArrCache = new HashMap<>();
    private Map<String, TypeDescription> schemaCache = new HashMap<>();
    private Map<String, String[]> columnNamesCache = new HashMap<>();

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
            Set<String> distinctShards = new HashSet<>();
            for (int i = 0; i < keyOrder.length; i++) {
                distinctShards.add(keyOrder[i]);
            }
            int maxFiles = (int) Math.pow(maxBucketSize, distinctShards.size());
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
                indexToFileNamesCache.remove(hhFilePath);
                indexToFileNamesCacheForFirstDimension.remove(hhFilePath);
                interMediateFileNamesCache.remove(hhFilePath);
                maxFilesCache.remove(hhFilePath);
                reduceFactorCache.remove(hhFilePath);
                hhfileToFileToNodeIdCache.remove(hhFilePath);
                hhfileNamesCalculatorCacheForFirstDimension.remove(hhFilePath);
                StatisticsFileHandler.INSTANCE.writeFileStatisticsMap(hhFilePath,fileStatisticsMapCache);
                fileStatisticsMapCache.remove(hhFilePath);
                fileStatisticsArrCache.remove(hhFilePath);
                columnNamesCache.remove(hhFilePath);
                schemaCache.remove(hhFilePath);
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

    public TypeDescription getSchema(String hhFilePath){
        TypeDescription schema = schemaCache.get(hhFilePath);
        if(schema==null){
            String[] colNames = getColumnNames(hhFilePath);
            schema = OrcSchemaCreator.createPrimitiveSchema(contextMap.get(hhFilePath).getConfiguredDataDescription(),colNames);
            schemaCache.put(hhFilePath,schema);
        }
        return schema;
    }

    public String[] getColumnNames(String hhFilePath){
        String[] columnNames = columnNamesCache.get(hhFilePath);
        if(columnNames==null){
            List<Column> columns = contextMap.get(hhFilePath).getShardingClientConfig().getInput().getDataDescription().getColumn();
            int numOfColumns = columns.size();
            columnNames = new String[numOfColumns];
            for (int i = 0; i < numOfColumns; i++) {
                columnNames[i] = columns.get(i).getName();
            }
            columnNamesCache.put(hhFilePath,columnNames);
        }
        return columnNames;
    }

    public Map<String,int[]> getFiletoNodeMap(String hhFilePath) {
        Map<String,int[]> filetoNodeMap = hhfileToFileToNodeIdCache.get(hhFilePath);
        if (filetoNodeMap == null) {
            return readLocationMetaData(hhFilePath);
        }
        return filetoNodeMap;
    }

    public HHFileNamesIdentifierForFirstDimension getHHFileNamesCalculatorForFirstDimension(String hhFilePath) {
        HHFileNamesIdentifierForFirstDimension hhFileNamesIdentifier = hhfileNamesCalculatorCacheForFirstDimension.get(hhFilePath);
        if (hhFileNamesIdentifier == null) {
            return generateHHFileNamesCalculatorForFirstDimension(hhFilePath);
        }
        return hhFileNamesIdentifier;
    }

    public FileStatistics[] getFileStatisticsMap(String hhFilePath) throws IOException, ClassNotFoundException {
        FileStatistics[] fileStatisticsArr = fileStatisticsArrCache.get(hhFilePath);
        if(fileStatisticsArr==null){
            return readFileStatisticsMap(hhFilePath);
        }
        return fileStatisticsArr;
    }

    public synchronized FileStatistics[] readFileStatisticsMap(String hhFilePath) throws IOException, ClassNotFoundException {
        FileStatistics[] fileStatisticsArr = fileStatisticsArrCache.get(hhFilePath);
        if (fileStatisticsArr == null) {
            String fileStatisticsFolderLocation = getFileStatisticsFolderLocation(hhFilePath);
            String blockStatisticsFolderLocation = getBlockStatisticsFolderLocation(hhFilePath);
            fileStatisticsArr = new FileStatistics[getMaxFiles(hhFilePath)];
            File fileStatistics = new File(fileStatisticsFolderLocation + File.separator + NodeInfo.INSTANCE.getId());
            SerializableComparator[] serializableComparators = contextMap.get(hhFilePath).getSerializableComparators();
            if (fileStatistics.exists()) {
                Map<String, FileStatistics> fileStatisticsMap = StatisticsFileHandler.INSTANCE.readFileStatistics(hhFilePath, fileStatisticsArr, blockStatisticsFolderLocation, fileStatistics, serializableComparators);
                fileStatisticsMapCache.put(hhFilePath, fileStatisticsMap);
            } else {
                StatisticsFileHandler.INSTANCE.createBlockStatisticsFolders(hhFilePath);
                initializeFileStatistics(hhFilePath, fileStatisticsArr, serializableComparators);
            }
        }
        fileStatisticsArrCache.put(hhFilePath, fileStatisticsArr);

        return fileStatisticsArr;
    }

    public void initializeFileStatistics(String hhFilePath, FileStatistics[] fileStatisticsArr, SerializableComparator[] serializableComparators) {
        Map<Integer,String> fileNames = getIndexToFileNamesForFirstDimension(hhFilePath);
        ShardingClientConfig shardingClientConfig = contextMap.get(hhFilePath).getShardingClientConfig();
        FieldTypeArrayDataDescription dataDescription = contextMap.get(hhFilePath).getConfiguredDataDescription();

        List<Column> columns = shardingClientConfig.getInput().getDataDescription().getColumn();
        int cols = dataDescription.getNumberOfDataFields();
        long recLen = dataDescription.getSize();
        int maxRecordsPerBlock= shardingClientConfig.getMaxRecordsPerBlock();
        Map<String, FileStatistics> fileStatisticsMap = new HashMap<>();
        for(Map.Entry<Integer,String> fileName : fileNames.entrySet()){
            fileStatisticsArr[fileName.getKey()] = new FileStatistics(columns,cols,dataDescription,recLen,maxRecordsPerBlock,serializableComparators);
            fileStatisticsMap.put(fileName.getValue(),fileStatisticsArr[fileName.getKey()]);
            fileStatisticsArr[fileName.getKey()].setBlockStatisticsList(new LinkedList<>());
        }
        fileStatisticsMapCache.put(hhFilePath,fileStatisticsMap);
    }

    private synchronized Map<String,int[]> generateHHFileNamesCalculator(String hhFilePath) {
        Map<String,int[]> filetoNodeMap = hhfileToFileToNodeIdCache.get(hhFilePath);
        if (filetoNodeMap == null) {
            ShardingApplicationContext context = contextMap.get(hhFilePath);
            filetoNodeMap = new HHFileNamesIdentifier(context.getShardingDimensions(),
                    keyToBucketToNodeMapCache.get(hhFilePath), Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize())).getFileToNodeMap();
            hhfileToFileToNodeIdCache.put(hhFilePath, filetoNodeMap);
        }
        return filetoNodeMap;
    }

    public synchronized Map<String,int[]> readLocationMetaData(String hhFilePath) {
        Map<String,int[]> fileNameToNodeId = null;
        if (fileNameToNodeId == null) {
            try (FileInputStream fileInputStream = new FileInputStream(FileSystemContext.getRootDirectory() + hhFilePath + File.separator + FileSystemConstants.FILE_LOCATION_INFO);
                 ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
                fileNameToNodeId = (Map<String, int[]>) objectInputStream.readObject();
                hhfileToFileToNodeIdCache.put(hhFilePath, fileNameToNodeId);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return fileNameToNodeId;
    }

    private synchronized HHFileNamesIdentifierForFirstDimension generateHHFileNamesCalculatorForFirstDimension(String hhFilePath) {
        HHFileNamesIdentifierForFirstDimension hhFileNamesIdentifier = hhfileNamesCalculatorCacheForFirstDimension.get(hhFilePath);
        if (hhFileNamesIdentifier == null) {
            ShardingApplicationContext context = contextMap.get(hhFilePath);
            hhFileNamesIdentifier = new HHFileNamesIdentifierForFirstDimension(context.getShardingDimensions(),
                    keyToBucketToNodeMapCache.get(hhFilePath), Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize()));
            hhfileNamesCalculatorCacheForFirstDimension.put(hhFilePath, hhFileNamesIdentifier);
        }
        return hhFileNamesIdentifier;
    }

    public Map<Integer, String> getIndexToFileNamesMap(String hhFilePath) {
        Map<Integer, String> fileNames = indexToFileNamesCache.get(hhFilePath);
        if (fileNames == null) {
            return generateIndexToFileNamesMap(hhFilePath);
        }
        return fileNames;
    }

    private synchronized Map<Integer, String> generateIndexToFileNamesMap(String hhFilePath) {
        Map<Integer, String> indexToFileNames = indexToFileNamesCache.get(hhFilePath);
        if (indexToFileNames == null) {
            ShardingApplicationContext context = contextMap.get(hhFilePath);
            NodeFileNamesIdentifier nodeFileNamesIdentifier = new NodeFileNamesIdentifier(context.getShardingDimensions(),
                    keyToBucketToNodeMapCache.get(hhFilePath), Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize()));
            indexToFileNames = nodeFileNamesIdentifier.getFileNames();
            indexToFileNamesCache.put(hhFilePath, indexToFileNames);
        }
        return indexToFileNames;
    }

    public Map<Integer, String> getIndexToFileNamesForFirstDimension(String hhFilePath) {
        Map<Integer, String> fileNames = indexToFileNamesCacheForFirstDimension.get(hhFilePath);
        if (fileNames == null) {
            return generateIndexToFileNamesForFirstDimension(hhFilePath);
        }
        return fileNames;
    }

    private synchronized Map<Integer, String> generateIndexToFileNamesForFirstDimension(String hhFilePath) {
        Map<Integer, String> indexToFileNames = indexToFileNamesCacheForFirstDimension.get(hhFilePath);
        if (indexToFileNames == null) {
            ShardingApplicationContext context = contextMap.get(hhFilePath);
            NodeFileNamesIdentifierForFirstDimension nodeFileNamesIdentifier = new NodeFileNamesIdentifierForFirstDimension(context.getShardingDimensions(),
                    keyToBucketToNodeMapCache.get(hhFilePath), Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize()));
            indexToFileNames = nodeFileNamesIdentifier.getIndexToFileNames();
            indexToFileNamesCacheForFirstDimension.put(hhFilePath, indexToFileNames);
        }
        logger.info("{} files for {}",indexToFileNames.size(),hhFilePath);
        return indexToFileNames;
    }

    public Map<Integer, String> getIndexToFileNamesMap(String hhFilePath, int fileId) {
        Map<Integer, Map<Integer, String>> intemediateFilesMap = interMediateFileNamesCache.get(hhFilePath);
        if (intemediateFilesMap == null || intemediateFilesMap.get(fileId) == null) {
            return generateIndexToFileNamesMap(hhFilePath, fileId);
        }
        return intemediateFilesMap.get(fileId);
    }


    private synchronized Map<Integer, String> generateIndexToFileNamesMap(String hhFilePath, int fileId) {
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


    private String getFileStatisticsFolderLocation(String hhFilePath) {
        String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
        String localDir = fileSystemBaseDirectory + hhFilePath;
        String fileStatisticsLocation =
                localDir + File.separatorChar + FileSystemConstants.FILE_STATISTICS_FOLDER_NAME;
        return fileStatisticsLocation;
    }

    private String getBlockStatisticsFolderLocation(String hhFilePath) {
        String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
        String localDir = fileSystemBaseDirectory + hhFilePath;
        String fileStatisticsLocation =
                localDir + File.separatorChar + FileSystemConstants.BLOCK_STATISTICS_FOLDER_NAME;
        return fileStatisticsLocation;
    }



}

