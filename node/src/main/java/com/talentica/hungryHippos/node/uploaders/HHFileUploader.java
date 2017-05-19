/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
package com.talentica.hungryHippos.node.uploaders;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.talentica.hungryHippos.node.DataDistributorStarter;
import com.talentica.hungryHippos.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;

/**
 * Created by rajkishoreh on 25/11/16.
 */
public enum HHFileUploader {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(HHFileUploader.class);

    private List<Node> nodes;

    private int noOfParallelThreads;

    HHFileUploader() {
        this.nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        noOfParallelThreads = nodes.size()/ DataDistributorStarter.noOfDataDistributors;
        if(noOfParallelThreads<1){
            noOfParallelThreads = 1;
        }else if(noOfParallelThreads > DataDistributorStarter.noOfDataDistributors){
            noOfParallelThreads = DataDistributorStarter.noOfDataDistributors;
        }
    }

    public void uploadFile(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath) throws IOException, InterruptedException {
        uploadFile(srcFolderPath,destinationPath,nodeToFileMap,hhFilePath,null);
    }

    public void uploadFile(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath, DataStore inMemoryDataStore) throws IOException, InterruptedException {
        LOGGER.info("Sending Replica Data To Nodes for {} from {}", destinationPath, srcFolderPath);
        int idx = 0;
        Map<Integer, DataInputStream> dataInputStreamMap = new ConcurrentHashMap<>();
        Map<Integer, Socket> socketMap = new ConcurrentHashMap<>();
        List<AbstractFileUploader> fileUploaders = new ArrayList<>();

        uploadFilesToCorrespondingNodes(srcFolderPath, destinationPath, nodeToFileMap, hhFilePath, idx, dataInputStreamMap, socketMap, fileUploaders, inMemoryDataStore);
        checkFilesUploadStatus(srcFolderPath, dataInputStreamMap, socketMap, fileUploaders);

        LOGGER.info("Completed Sending Replica Data To Nodes for {} from ", destinationPath, srcFolderPath);
    }

    private void uploadFilesToCorrespondingNodes(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath, int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, List<AbstractFileUploader> fileUploaders, DataStore dataStore) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(this.nodes.size());
        ExecutorService fileUploadService = Executors.newFixedThreadPool(noOfParallelThreads);
        if(dataStore instanceof FileDataStore){
            uploadForFileDataStore(srcFolderPath, destinationPath, nodeToFileMap, hhFilePath, idx, dataInputStreamMap, socketMap, fileUploaders, countDownLatch, fileUploadService);
        }else if(dataStore instanceof InMemoryDataStore){
            uploadForInMemoryDataStore(srcFolderPath, destinationPath, nodeToFileMap, hhFilePath, idx, dataInputStreamMap, socketMap, fileUploaders,(InMemoryDataStore) dataStore, countDownLatch, fileUploadService);
        }else if(dataStore instanceof NodeWiseDataStore){
            uploadForNodeWiseDataStore(srcFolderPath, destinationPath, nodeToFileMap, hhFilePath, idx, dataInputStreamMap, socketMap, fileUploaders, countDownLatch, fileUploadService);
        }else{
            uploadForHybridDataStore(srcFolderPath, destinationPath, nodeToFileMap, hhFilePath, idx, dataInputStreamMap, socketMap, fileUploaders,(HybridDataStore) dataStore, countDownLatch, fileUploadService);
        }

        countDownLatch.await();
        fileUploadService.shutdown();
    }

    private void uploadForHybridDataStore(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath, int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, List<AbstractFileUploader> fileUploaders, HybridDataStore hybridDataStore, CountDownLatch countDownLatch, ExecutorService fileUploadService) {
        for (Node node : this.nodes) {
            int nodeId = node.getIdentifier();
            Set<String> fileNames = nodeToFileMap.get(nodeId);
            if (fileNames != null && !fileNames.isEmpty()) {
                String tarFilename =  UUID.randomUUID().toString() + ".tar";
                AbstractFileUploader fileUploader  = new HybridFileUploader(countDownLatch,srcFolderPath, destinationPath, idx, dataInputStreamMap, socketMap, node, fileNames, hhFilePath, hybridDataStore,tarFilename);
                fileUploaders.add(fileUploader);
                fileUploadService.execute(fileUploader);
                idx++;
            }else{
                countDownLatch.countDown();
            }
        }
    }

    private void uploadForNodeWiseDataStore(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath, int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, List<AbstractFileUploader> fileUploaders, CountDownLatch countDownLatch, ExecutorService fileUploadService) {
        for (Node node : this.nodes) {
            int nodeId = node.getIdentifier();
            Set<String> fileNames = nodeToFileMap.get(nodeId);
            if (fileNames != null && !fileNames.isEmpty()) {
                String fileName =  nodeId+"";
                AbstractFileUploader fileUploader  = new NodeWiseFileUploader(countDownLatch,srcFolderPath, destinationPath, idx, dataInputStreamMap, socketMap, node, fileNames, hhFilePath, fileName);
                fileUploaders.add(fileUploader);
                fileUploadService.execute(fileUploader);
                idx++;
            }else{
                countDownLatch.countDown();
            }
        }
    }

    private void uploadForInMemoryDataStore(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath, int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, List<AbstractFileUploader> fileUploaders, InMemoryDataStore inMemoryDataStore, CountDownLatch countDownLatch, ExecutorService fileUploadService) {
        for (Node node : this.nodes) {
            int nodeId = node.getIdentifier();
            Set<String> fileNames = nodeToFileMap.get(nodeId);
            if (fileNames != null && !fileNames.isEmpty()) {
                String tarFilename =  UUID.randomUUID().toString() + ".tar";
                AbstractFileUploader fileUploader  = new InMemoryFileUploader(countDownLatch,srcFolderPath, destinationPath, idx, dataInputStreamMap, socketMap, node, fileNames, hhFilePath, inMemoryDataStore,tarFilename);
                fileUploaders.add(fileUploader);
                fileUploadService.execute(fileUploader);
                idx++;
            }else{
                countDownLatch.countDown();
            }
        }
    }

    private void uploadForFileDataStore(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath, int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, List<AbstractFileUploader> fileUploaders, CountDownLatch countDownLatch, ExecutorService fileUploadService) {
        for (Node node : this.nodes) {
            int nodeId = node.getIdentifier();
            Set<String> fileNames = nodeToFileMap.get(nodeId);
            if (fileNames != null && !fileNames.isEmpty()) {
                String tarFilename =  UUID.randomUUID().toString() + ".tar";
                AbstractFileUploader fileUploader  = new FileUploader(countDownLatch,srcFolderPath, destinationPath, idx, dataInputStreamMap, socketMap, node, fileNames, hhFilePath,tarFilename);
                fileUploaders.add(fileUploader);
                fileUploadService.execute(fileUploader);
                idx++;
            }else{
                countDownLatch.countDown();
            }
        }
    }

    private void checkFilesUploadStatus(String srcFolderPath, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, List<AbstractFileUploader> fileUploaders) throws IOException {
        boolean success = true;
        success = checkFileUploadersStatuses(srcFolderPath, fileUploaders, success);

        success = checkFilesUploadRequestStatuses(srcFolderPath, dataInputStreamMap, socketMap, success);
        if (!success) {
            throw new RuntimeException("File Upload Failed for " + srcFolderPath);
        }
    }

    private boolean checkFileUploadersStatuses(String srcFolderPath, List<AbstractFileUploader> fileUploaders, boolean success) {
        for(AbstractFileUploader fileUploader:fileUploaders){
            success = success&&fileUploader.isSuccess();
        }
        if (!success) {
            throw new RuntimeException("File Upload Failed for " + srcFolderPath);
        }
        return success;
    }

    private boolean checkFilesUploadRequestStatuses(String srcFolderPath, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, boolean success) throws IOException {
        for (Map.Entry<Integer, Socket> entry : socketMap.entrySet()) {
            LOGGER.info("Waiting for status from {} for {}", entry.getValue().getInetAddress(), srcFolderPath);
            String status = dataInputStreamMap.get(entry.getKey()).readUTF();
            LOGGER.info("Success Status from {} for {} is {}", entry.getValue().getInetAddress(), srcFolderPath, status);
            entry.getValue().close();
            if (!HungryHippoServicesConstants.SUCCESS.equals(status)) {
                success = false;
            }
        }
        return success;
    }



}
