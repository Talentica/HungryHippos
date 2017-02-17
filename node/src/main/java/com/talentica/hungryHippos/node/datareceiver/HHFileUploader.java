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

    public static final String SCRIPT_FOR_TAR_FILE_LIST = "tar-file-list.sh";

    private String hungryHippoBinDir;

    private List<Node> nodes;

    private ExecutorService fileUploadService;

    HHFileUploader() {
        this.hungryHippoBinDir = System.getProperty("hh.bin.dir");
        this.nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        this.fileUploadService = Executors.newFixedThreadPool(nodes.size());
    }

    public void uploadFile(String srcFolderPath, String destinationPath, Map<Integer, Set<String>> nodeToFileMap, String hhFilePath) throws IOException, InterruptedException {
        LOGGER.info("Inside uploadFile for {} from {}", destinationPath, srcFolderPath);
        LOGGER.info("Sending Replica Data To Nodes for {}", destinationPath);
        int idx = 0;
        Map<Integer, DataInputStream> dataInputStreamMap = new ConcurrentHashMap<>();
        Map<Integer, Socket> socketMap = new ConcurrentHashMap<>();
        List<FileUploader> fileUploaders = new ArrayList<>();

        CountDownLatch countDownLatch = new CountDownLatch(this.nodes.size());
        for (com.talentica.hungryhippos.config.cluster.Node node : this.nodes) {
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
        boolean success = true;
        for(FileUploader fileUploader:fileUploaders){
            success = success&&fileUploader.isSuccess();
        }
        if (!success) {
            throw new RuntimeException("File Upload Failed for " + srcFolderPath);
        }

        for (Map.Entry<Integer, Socket> entry : socketMap.entrySet()) {
            LOGGER.info("Waiting for status from {} for {}", entry.getValue().getInetAddress(), srcFolderPath);
            String status = dataInputStreamMap.get(entry.getKey()).readUTF();
            LOGGER.info("Success Status from {} for {} is {}", entry.getValue().getInetAddress(), srcFolderPath, status);
            entry.getValue().close();
            if (!HungryHippoServicesConstants.SUCCESS.equals(status)) {
                success = false;
            }
            
        }
        if (!success) {
            throw new RuntimeException("File Upload Failed for " + srcFolderPath);
        }

        LOGGER.info("Completed Sending Replica Data To Nodes for {} from ", destinationPath, srcFolderPath);
    }

}
