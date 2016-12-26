package com.talentica.hungryHippos.master;

import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * Created by rajkishoreh on 19/12/16.
 */
public class FileSplitterAndUploader implements Runnable{

    private static Logger logger = LoggerFactory.getLogger(FileSplitterAndUploader.class);
    int chunkIdx;
    List<Node> nodes;
    String destinationPath;
    String remotePath;
    Map<Integer, DataInputStream> dataInputStreamMap;
    Map<Integer, Socket> socketMap;
    String chunkFilePathPrefix;
    String chunkFileNamePrefix;
    String commonCommandArgs;
    boolean success;


    public FileSplitterAndUploader(int chunkIdx, List<Node> nodes, String destinationPath, String remotePath, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, String chunkFilePathPrefix, String chunkFileNamePrefix, String commonCommandArgs) {
        this.chunkIdx = chunkIdx;
        this.nodes = nodes;
        this.destinationPath = destinationPath;
        this.remotePath = remotePath;
        this.dataInputStreamMap = dataInputStreamMap;
        this.socketMap = socketMap;
        this.chunkFilePathPrefix = chunkFilePathPrefix;
        this.chunkFileNamePrefix = chunkFileNamePrefix;
        this.commonCommandArgs = commonCommandArgs;
        this.success = false;
    }

    @Override
    public void run() {

        int currentChunk = chunkIdx + 1;

        String chunkFilePath = chunkFilePathPrefix + chunkIdx;
        String chunkFileName = chunkFileNamePrefix + chunkIdx;
        File file = new File(chunkFilePath);
        try {
            logger.info("[{}] Creating chunk {}",Thread.currentThread().getName(),currentChunk);
            Process process  = Runtime.getRuntime().exec(commonCommandArgs + " " + currentChunk + " " + chunkFilePath);
            file.deleteOnExit();
            int processStatus = process.waitFor();
            if (processStatus != 0) {
                String line = "";
                BufferedReader errReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                while ((line = errReader.readLine()) != null) {
                    logger.error(line);
                }
                errReader.close();
                logger.error("[{}] {} File publish failed for {}",Thread.currentThread().getName(),chunkFilePath, destinationPath);
                throw new RuntimeException("File Publish failed");
            }
            int nodeId = chunkIdx % nodes.size();
            logger.info("[{}] Uploading chunk {}",Thread.currentThread().getName(),currentChunk);
            DataPublisherStarter.uploadChunk(destinationPath, nodes, remotePath, dataInputStreamMap, socketMap, chunkIdx, chunkFilePath, chunkFileName, nodeId);
            this.success = true;
            logger.info("[{}] Upload is successful for chunk {}",Thread.currentThread().getName(),currentChunk);
        } catch (IOException |InterruptedException e) {
            e.printStackTrace();
            logger.error("[{}] {} File publish failed for {}",Thread.currentThread().getName(),chunkFilePath, destinationPath);
            throw new RuntimeException("File Publish failed");
        }
        file.delete();
    }

    public boolean isSuccess() {
        return success;
    }
}