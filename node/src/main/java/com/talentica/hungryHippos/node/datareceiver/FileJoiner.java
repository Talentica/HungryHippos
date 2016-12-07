package com.talentica.hungryHippos.node.datareceiver;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by rajkishoreh on 23/11/16.
 */
public class FileJoiner {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileJoiner.class);

    private static Map<String, Object> lockMap = new ConcurrentHashMap<>();

    private static Object classLock = new Object();

    public static void join(String srcFolderPath, String destFolderPath) throws IOException, InterruptedException {
        LOGGER.info("Moving data from {} to {}", srcFolderPath, destFolderPath);
        Object lock = lockMap.get(destFolderPath);
        if (lock == null) {
            synchronized (classLock) {
                lock = lockMap.get(destFolderPath);
                if (lock == null) {
                    lock = new Object();
                    lockMap.put(destFolderPath, lock);
                }
            }
        }
        synchronized (lock) {
            File srcFolder = new File(srcFolderPath);
            File destFolder = new File(destFolderPath);
            if (!destFolder.exists()) {
                destFolder.mkdir();
            }
            if (srcFolder.exists()) {

                String[] srcFilePaths = srcFolder.list();
                for (int i = 0; i < srcFilePaths.length; i++) {
                    File srcFile = new File(srcFolderPath + File.separator + srcFilePaths[i]);
                    File destFile = new File(destFolderPath + File.separator + srcFile.getName());
                    if(destFile.exists()){
                        appendData(srcFile, destFile);
                        srcFile.delete();
                    }else{
                        srcFile.renameTo(destFile);
                    }
                }
                LOGGER.info("Deleting data from {}", srcFolderPath);
                FileUtils.forceDelete(new File(srcFolderPath));
            }
        }
        lockMap.remove(destFolderPath);
        LOGGER.info("Completed Moving data from {} to {}", srcFolderPath, destFolderPath);
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
