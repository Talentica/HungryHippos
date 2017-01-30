package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryhippos.config.cluster.Node;
import org.apache.commons.io.IOUtils;
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
 * Created by rajkishoreh on 23/11/16.
 */
public enum FileJoiner {
    INSTANCE;

    private static final Logger LOGGER = LoggerFactory.getLogger(FileJoiner.class);

    private Map<String, AtomicInteger> lockMap;

    private List<Node> nodes;

    private ExecutorService metadataUploaderService;

    FileJoiner() {
        this.nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        this.metadataUploaderService = Executors.newFixedThreadPool(nodes.size());
        this.lockMap = new ConcurrentHashMap<>();
    }

    public void join(String srcFolderPath, String destFolderPath, String lockString) throws IOException, InterruptedException {
        join(srcFolderPath, destFolderPath, lockString, null,null);
    }

    public void join(String srcFolderPath, String destFolderPath, String lockString, String metadataFilePath, String hhFilePath) throws IOException, InterruptedException {
        LOGGER.info("Moving data from {} to {}", srcFolderPath, destFolderPath);

        AtomicInteger lock = getLock(lockString);

        synchronized (lock) {
            File srcFolder = new File(srcFolderPath);
            File destFolder = new File(destFolderPath);
            if (!destFolder.exists()) {
                destFolder.mkdir();
            }
            if (srcFolder.exists()) {
                String[] srcFilePaths = srcFolder.list();
                if (metadataFilePath != null) {
                    Map<String, Long> fileNameToSizeMap = null;
                    File metadataFile = new File(metadataFilePath);
                    if (metadataFile.exists()) {
                        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(metadataFile));
                        try {
                            LOGGER.info("Reading metadata for {}", srcFolderPath);
                            fileNameToSizeMap = (Map<String, Long>) objectInputStream.readObject();
                            LOGGER.info("Completed reading metadata for {}", srcFolderPath);
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                        objectInputStream.close();
                    } else if (!metadataFile.getParentFile().exists()) {
                        LOGGER.info("Creating metadata folder for {}", srcFolderPath);
                        metadataFile.getParentFile().mkdirs();
                    }
                    if (fileNameToSizeMap == null) {
                        fileNameToSizeMap = new HashMap<>();
                    }
                    for (int i = 0; i < srcFilePaths.length; i++) {

                        File srcFile = new File(srcFolderPath + File.separator + srcFilePaths[i]);
                        File destFile = new File(destFolderPath + File.separator + srcFile.getName());
                        if (!destFile.exists()) {
                            destFile.createNewFile();
                        }
                        fileNameToSizeMap.put(srcFile.getName(), srcFile.length() + destFile.length());
                        appendData(srcFile, destFile);
                    }
                    LOGGER.info("Writing metadata for {}", srcFolderPath);
                    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metadataFilePath, false));
                    oos.writeObject(fileNameToSizeMap);
                    oos.flush();
                    oos.close();
                    LOGGER.info("Completed writing metadata for {}", srcFolderPath);
                    LOGGER.info("Uploading metadata for {}", srcFolderPath);
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

                    LOGGER.info("Completed Uploading metadata for {}", srcFolderPath);
                } else {
                    for (int i = 0; i < srcFilePaths.length; i++) {
                        File srcFile = new File(srcFolderPath + File.separator + srcFilePaths[i]);
                        File destFile = new File(destFolderPath + File.separator + srcFile.getName());
                        if (!destFile.exists()) {
                            destFile.createNewFile();
                        }
                        appendData(srcFile, destFile);
                    }
                }
            }
        }
        releaseLock(lockString);

        LOGGER.info("Completed Moving data from {} to {}", srcFolderPath, destFolderPath);
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

    private static void appendData(File srcFile, File destFile) throws IOException {
        long srcFileLength = srcFile.length();
        if (srcFileLength != 0) {
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(srcFile));
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(destFile, true));
            IOUtils.copy(bufferedInputStream, bufferedOutputStream);
            bufferedOutputStream.flush();
            bufferedInputStream.close();
            bufferedOutputStream.close();
        }
    }
}
