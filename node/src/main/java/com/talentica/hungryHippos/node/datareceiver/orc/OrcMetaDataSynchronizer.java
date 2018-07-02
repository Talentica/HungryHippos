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
package com.talentica.hungryHippos.node.datareceiver.orc;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.ApplicationCache;
import com.talentica.hungryHippos.node.datareceiver.DataUpdaterClient;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.FileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public enum OrcMetaDataSynchronizer {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(OrcMetaDataSynchronizer.class);

    private List<Node> nodes;
    private Map<Integer,Node> nodeMap;
    private List<Object> locks;
    private int noOfLocks;

    OrcMetaDataSynchronizer() {
        this.nodes = new LinkedList<>();
        this.nodeMap = new HashMap<>();
        for (Node node : CoordinationConfigUtil.getZkClusterConfigCache().getNode()) {
            if (node.getIdentifier() != NodeInfo.INSTANCE.getIdentifier()) {
                nodes.add(node);
            }
            nodeMap.put(node.getIdentifier(),node);
        }
        Collections.shuffle(nodes);
        this.locks = new ArrayList<>();
        this.noOfLocks = 100;
        for (int i = 0; i < noOfLocks; i++) {
            locks.add(new Object());
        }
    }

    public void synchronize(String dataFolderPath, Collection<String> fileNames, String metaDataFilePath, String hhFilePath) throws IOException, InterruptedException, ExecutionException {
        LOGGER.info("Updating meta data of {}", hhFilePath);
        Object lockObject = locks.get((hhFilePath.hashCode() % noOfLocks + noOfLocks) % noOfLocks);
        synchronized (lockObject) {
            File srcFolder = new File(dataFolderPath);
            if (srcFolder.exists()) {
                List<Future<Boolean>> futures = new LinkedList<>();
                ExecutorService executorService = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors());
                try {
                    Map<String, int[]>  fileToNodeMap = ApplicationCache.INSTANCE.getFiletoNodeMap(hhFilePath);
                    writeMetaDataIntoFile(dataFolderPath, fileNames, metaDataFilePath,fileToNodeMap);
                    syncSingleFile(hhFilePath, metaDataFilePath, futures, executorService);
                    for (Future<Boolean> future : futures) {
                        if (!future.get()) {
                            throw new RuntimeException("MetaData update failed");
                        }
                    }
                } finally {
                    executorService.shutdown();
                }
            }
        }
        LOGGER.info("Completed updating data of {}", hhFilePath);
    }



    private void syncSingleFile(String hhFilePath, String fileStatisticsPath, List<Future<Boolean>> futures, ExecutorService executorService) {
        for (Node node : nodes) {
            Queue<String> queue = new ConcurrentLinkedQueue<>();
            queue.offer(fileStatisticsPath);
            futures.add(executorService.submit(new DataUpdaterClient(hhFilePath, node, queue)));
        }
    }


    private void writeMetaDataIntoFile(String dataFolderPath, Collection<String> fileNames, String metadataFilePath, Map<String, int[]>  fileToNodeMap) throws IOException {
        Map<String,FileInfo> fileNameToSizesMap = new HashMap<>();
        File metadataFile = new File(metadataFilePath);

        for (String fileName : fileNames) {
            String mainFilePathTail = fileName + File.separator+FileSystemConstants.ORC_MAIN_FILE_NAME;
            String deltaFilePathTail = fileName + File.separator+FileSystemConstants.ORC_DELTA_FILE_NAME;

            File mainFile = new File(dataFolderPath + mainFilePathTail);
            File deltaFile = new File(dataFolderPath + deltaFilePathTail);

            fileNameToSizesMap.put(mainFilePathTail,new FileInfo(mainFile.length(),mainFile.lastModified()));
            fileNameToSizesMap.put(deltaFilePathTail,new FileInfo(deltaFile.length(),deltaFile.lastModified()));

        }
        LOGGER.info("Writing metadata for {}", dataFolderPath);
        metadataFile.delete();
        FileOutputStream fos = new FileOutputStream(metadataFilePath, false);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(fileNameToSizesMap);
        oos.flush();
        fos.flush();
        oos.close();
        fos.close();
        LOGGER.info("Completed writing metadata for {}", dataFolderPath);
    }


}
