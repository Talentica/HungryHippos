package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rajkishoreh on 6/2/17.
 */
public enum MetaDataSynchronizer {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataSynchronizer.class);

    private Map<String, AtomicInteger> lockMap;

    private List<Node> nodes;

    private ExecutorService metadataUploaderService;

    MetaDataSynchronizer() {
        this.nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        this.metadataUploaderService = Executors.newFixedThreadPool(nodes.size());
        this.lockMap = new ConcurrentHashMap<>();
    }

    public void synchronize(String dataFolderPath, String[] fileNames, String lockString, String metadataFilePath, String hhFilePath) throws IOException, InterruptedException {
        LOGGER.info("Updating meta data of {}", dataFolderPath);

        AtomicInteger lock = getLock(lockString);

        synchronized (lock) {
            File srcFolder = new File(dataFolderPath);
            if (srcFolder.exists()) {
                if (metadataFilePath != null) {
                    Map<String, Long> fileNameToSizeMap = new HashMap<>();
                    File metadataFile = new File(metadataFilePath);
                    if (!metadataFile.getParentFile().exists()) {
                        LOGGER.info("Creating metadata folder for {}", dataFolderPath);
                        metadataFile.getParentFile().mkdirs();
                    }
                    for (int i = 0; i < fileNames.length; i++) {
                        File file = new File(dataFolderPath + File.separator + fileNames[i]);
                        fileNameToSizeMap.put(fileNames[i], file.length());
                    }
                    LOGGER.info("Writing metadata for {}", dataFolderPath);
                    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metadataFilePath, false));
                    oos.writeObject(fileNameToSizeMap);
                    oos.flush();
                    oos.close();
                    LOGGER.info("Completed writing metadata for {}", dataFolderPath);
                    LOGGER.info("Uploading metadata for {}", dataFolderPath);
                    MetaDataUploader[] metaDataUploaders = new MetaDataUploader[nodes.size() - 1];
                    CountDownLatch countDownLatch = new CountDownLatch(metaDataUploaders.length);
                    int idx = 0;
                    for (Node node : nodes) {
                        if (node.getIdentifier() != NodeInfo.INSTANCE.getIdentifier()) {
                            MetaDataUploader metaDataUploader = new MetaDataUploader(countDownLatch, node, metadataFilePath, hhFilePath);
                            metaDataUploaders[idx] = metaDataUploader;
                            metadataUploaderService.execute(metaDataUploader);
                            idx++;
                        }
                    }
                    countDownLatch.await();
                    boolean metadataUpdateSuccessStatus = true;
                    for (int i = 0; i < metaDataUploaders.length; i++) {
                        metadataUpdateSuccessStatus = metadataUpdateSuccessStatus && metaDataUploaders[i].isSuccess();
                        if (!metadataUpdateSuccessStatus) {
                            throw new RuntimeException("Metadata update failed for "+metaDataUploaders[i].getNode().getIp());
                        }
                    }

                    LOGGER.info("Completed Uploading metadata for {}", dataFolderPath);
                }
            }
        }
        releaseLock(lockString);

        LOGGER.info("Completed updating data of {}", dataFolderPath);
    }

    private AtomicInteger getLock(String lockString){
        return processLock(lockString,false);
    }

    private void releaseLock(String lockString){
        processLock(lockString,true);
    }

    private synchronized AtomicInteger processLock(String lockString, boolean releaseFlag) {
        AtomicInteger lock = lockMap.get(lockString);
        if (releaseFlag) {
            if (lock!=null && lock.decrementAndGet() <= 0) {
                lockMap.remove(lockString);
            }
        } else if (lock == null) {
            lock = new AtomicInteger(0);
            lockMap.put(lockString, lock);
        }
        lock.getAndIncrement();
        return lock;
    }
}