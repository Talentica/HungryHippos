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

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.BlockStatistics;
import com.talentica.hungryhippos.filesystem.FileStatistics;
import com.talentica.hungryhippos.filesystem.SerializableComparator;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public enum StatisticsFileHandler {
    INSTANCE;

    public void createBlockStatisticsFolders(String hhFilePath){
        for (Node node : CoordinationConfigUtil.getZkClusterConfigCache().getNode()) {
            new File(getBlockStatisticsFolderLocation(hhFilePath) + File.separator+node.getIdentifier()).mkdir();
        }
    }
    public void writeFileStatisticsMap(String hhFilePath, Map<String, Map<String, FileStatistics>> fileStatisticsMapCache) {
        if (fileStatisticsMapCache.get(hhFilePath) != null) {
            String ownedBlockStatisticsFolderPath = getBlockStatisticsFolderLocation(hhFilePath) + File.separator + NodeInfo.INSTANCE.getId()+File.separator;
            ExecutorService executorService = Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors());
            try{
                List<Future<Boolean>> futures= new LinkedList<>();
                for (Map.Entry<String, FileStatistics> fileStatisticsEntry : fileStatisticsMapCache.get(hhFilePath).entrySet()) {
                    if(fileStatisticsEntry.getValue().getDataSize()>0){
                        futures.add(executorService.submit(new BlockStatisticsWriter(ownedBlockStatisticsFolderPath, fileStatisticsEntry)));
                    }
                }
                for(Future<Boolean> future:futures){
                    if(!future.get()){
                       throw new RuntimeException("BlockStatistics write failure");
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } finally {
                executorService.shutdown();
            }
            try (FileOutputStream fileOutputStream = new FileOutputStream(
                    getFileStatisticsFolderLocation(hhFilePath) + File.separator + NodeInfo.INSTANCE.getId());
                 ObjectOutputStream oos = new ObjectOutputStream(fileOutputStream)) {
                oos.writeObject(fileStatisticsMapCache.get(hhFilePath));
                oos.flush();
                fileOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Map<String, FileStatistics> readFileStatistics(String hhFilePath, FileStatistics[] fileStatisticsArr, String blockStatisticsFolderLocation, File fileStatistics, SerializableComparator[] serializableComparators) throws IOException, ClassNotFoundException {
        Map<String, FileStatistics> fileStatisticsMap = null;
        try (FileInputStream fileInputStream = new FileInputStream(fileStatistics);
             BufferedInputStream bis = new BufferedInputStream(fileInputStream);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            fileStatisticsMap = (Map<String, FileStatistics>) ois.readObject();
        }
        Map<Integer, String> indexToFileNames = ApplicationCache.INSTANCE.getIndexToFileNamesForFirstDimension(hhFilePath);
        String ownedBlockStatisticsFolderPath = blockStatisticsFolderLocation + File.separator + NodeInfo.INSTANCE.getId()+File.separator;
        Map<String,Future<List<BlockStatistics>>> futures = new HashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors());
        try{
            Queue<String> futureQueue = new LinkedList<>();
            for (Map.Entry<Integer, String> indexToFileName : indexToFileNames.entrySet()) {
                String fileName = indexToFileName.getValue();
                fileStatisticsArr[indexToFileName.getKey()] = fileStatisticsMap.get(fileName);
                if(fileStatisticsMap.get(fileName).getDataSize()>0){
                    Future<List<BlockStatistics>> future =executorService.submit(new BlockStatisticsReader(ownedBlockStatisticsFolderPath+fileName));
                    futures.put(fileName,future);
                    while(!futureQueue.offer(fileName));
                }else{
                    fileStatisticsMap.get(fileName).setBlockStatisticsList(new LinkedList<>());
                    fileStatisticsMap.get(fileName).setComparators(serializableComparators);
                }
            }
            while(!futureQueue.isEmpty()){
                 String fileName = futureQueue.poll();
                Future<List<BlockStatistics>> future = futures.get(fileName);
                if(future.isDone()){
                    fileStatisticsMap.get(fileName).setBlockStatisticsList(future.get());
                    fileStatisticsMap.get(fileName).setComparators(serializableComparators);
                }else{
                    while(!futureQueue.offer(fileName));
                }
            }
        } catch (InterruptedException |ExecutionException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }

        return fileStatisticsMap;
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
