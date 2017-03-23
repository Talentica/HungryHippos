/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.node.datareceiver;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private ExecutorService fileUploadService;

    HHFileUploader() {
        this.nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        this.fileUploadService = Executors.newFixedThreadPool(nodes.size());
    }

    public void uploadFile(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath) throws IOException, InterruptedException {
        LOGGER.info("Sending Replica Data To Nodes for {} from {}", destinationPath, srcFolderPath);
        int idx = 0;
        Map<Integer, DataInputStream> dataInputStreamMap = new ConcurrentHashMap<>();
        Map<Integer, Socket> socketMap = new ConcurrentHashMap<>();
        List<FileUploader> fileUploaders = new ArrayList<>();

        uploadFilesToCorrespondingNodes(srcFolderPath, destinationPath, nodeToFileMap, hhFilePath, idx, dataInputStreamMap, socketMap, fileUploaders);
        checkFilesUploadStatus(srcFolderPath, dataInputStreamMap, socketMap, fileUploaders);

        LOGGER.info("Completed Sending Replica Data To Nodes for {} from ", destinationPath, srcFolderPath);
    }

    private void uploadFilesToCorrespondingNodes(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath, int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, List<FileUploader> fileUploaders) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(this.nodes.size());
        for (Node node : this.nodes) {
            int nodeId = node.getIdentifier();
            Set<String> fileNames = nodeToFileMap.get(nodeId);
            if (fileNames != null && !fileNames.isEmpty()) {
                FileUploader fileUploader  = new FileUploader(countDownLatch,srcFolderPath, destinationPath, idx, dataInputStreamMap, socketMap, node, fileNames, hhFilePath);
                fileUploaders.add(fileUploader);
                fileUploadService.execute(fileUploader);
                idx++;
            }else{
                countDownLatch.countDown();
            }
        }
        countDownLatch.await();
    }

    private void checkFilesUploadStatus(String srcFolderPath, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, List<FileUploader> fileUploaders) throws IOException {
        boolean success = true;
        success = checkFileUploadersStatuses(srcFolderPath, fileUploaders, success);

        success = checkFilesUploadRequestStatuses(srcFolderPath, dataInputStreamMap, socketMap, success);
        if (!success) {
            throw new RuntimeException("File Upload Failed for " + srcFolderPath);
        }
    }

    private boolean checkFileUploadersStatuses(String srcFolderPath, List<FileUploader> fileUploaders, boolean success) {
        for(FileUploader fileUploader:fileUploaders){
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
