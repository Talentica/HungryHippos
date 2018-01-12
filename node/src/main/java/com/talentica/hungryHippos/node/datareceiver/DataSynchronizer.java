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
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public enum DataSynchronizer {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSynchronizer.class);

    private List<Node> nodes;
    private List<Object> locks;
    private int noOfLocks;

    DataSynchronizer() {
        this.nodes = new LinkedList<>();
        for (Node node : CoordinationConfigUtil.getZkClusterConfigCache().getNode()) {
            if (node.getIdentifier() != NodeInfo.INSTANCE.getIdentifier()) {
                nodes.add(node);
            }
        }
        Collections.shuffle(nodes);
        this.locks = new ArrayList<>();
        this.noOfLocks = 100;
        for (int i = 0; i < noOfLocks; i++) {
            locks.add(new Object());
        }
    }

    public void synchronize(String dataFolderPath, Collection<String> fileNames, String metaDataFilePath, String hhFilePath, String fileStatisticsPath, String blockStatisticsFolderPath) throws IOException, InterruptedException, ExecutionException {
        LOGGER.info("Updating meta data of {}", hhFilePath);
        Object lockObject = locks.get((hhFilePath.hashCode() % noOfLocks + noOfLocks) % noOfLocks);
        synchronized (lockObject) {
            File srcFolder = new File(dataFolderPath);
            if (srcFolder.exists()) {
                List<Future<Boolean>> futures = new LinkedList<>();
                ExecutorService executorService = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors());
                try {
                    File blockStatisticFolder = new File(blockStatisticsFolderPath);
                    if(blockStatisticFolder.exists()){
                        syncSingleFile(hhFilePath, fileStatisticsPath, futures, executorService);
                        TarAndGzip.folder(blockStatisticFolder);
                        syncBlockStatistics(hhFilePath, blockStatisticsFolderPath, futures, executorService);
                    }
                    writeMetaDataIntoFile(dataFolderPath, fileNames, metaDataFilePath);
                    syncSingleFile(hhFilePath, metaDataFilePath, futures, executorService);
                    for (Future<Boolean> future : futures) {
                        if (!future.get()) {
                            throw new RuntimeException("MetaData update failed");
                        }
                    }
                    new File(blockStatisticsFolderPath+".tar.gz");
                } finally {
                    executorService.shutdown();
                }
            }
        }
        LOGGER.info("Completed updating data of {}", hhFilePath);
    }

    private void syncBlockStatistics(String hhFilePath, String blockStatisticsFolderPath, List<Future<Boolean>> futures, ExecutorService executorService) {
        for (Node node : nodes) {
            futures.add(executorService.submit(new BlockStatisticsDataUpdaterClient(hhFilePath, node, blockStatisticsFolderPath)));
        }
    }

    private void syncSingleFile(String hhFilePath, String fileStatisticsPath, List<Future<Boolean>> futures, ExecutorService executorService) {
        for (Node node : nodes) {
            Queue<String> queue = new ConcurrentLinkedQueue<>();
            queue.offer(fileStatisticsPath);
            futures.add(executorService.submit(new DataUpdaterClient(hhFilePath, node, queue)));
        }
    }


    private void writeMetaDataIntoFile(String dataFolderPath, Collection<String> fileNames, String metadataFilePath) throws IOException {
        Map<String, Long> fileNameToSizeMap = new HashMap<>();
        File metadataFile = new File(metadataFilePath);
        for (String fileName : fileNames) {
            File file = new File(dataFolderPath + fileName + FileSystemConstants.ZIP_EXTENSION);
            fileNameToSizeMap.put(fileName, file.length());
        }
        LOGGER.info("Writing metadata for {}", dataFolderPath);
        metadataFile.delete();
        FileOutputStream fos = new FileOutputStream(metadataFilePath, false);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(fileNameToSizeMap);
        oos.flush();
        fos.flush();
        oos.close();
        fos.close();
        LOGGER.info("Completed writing metadata for {}", dataFolderPath);
    }


}
