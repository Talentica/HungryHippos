package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rajkishoreh on 26/12/16.
 */
public class FileUploader implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FileUploader.class);
    private CountDownLatch countDownLatch;
    private String srcFolderPath, destinationPath, remoteTargetFolder, commonCommandArg;
    private int idx;
    private Map<Integer, DataInputStream> dataInputStreamMap;
    private Map<Integer, Socket> socketMap;
    private Node node;
    private Set<String> fileNames;
    private boolean success;


    public FileUploader(CountDownLatch countDownLatch, String srcFolderPath, String destinationPath, String remoteTargetFolder, String commonCommandArg, int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Node node, Set<String> fileNames) {
        this.countDownLatch = countDownLatch;
        this.srcFolderPath = srcFolderPath;
        this.destinationPath = destinationPath;
        this.remoteTargetFolder = remoteTargetFolder;
        this.commonCommandArg = commonCommandArg;
        this.idx = idx;
        this.dataInputStreamMap = dataInputStreamMap;
        this.socketMap = socketMap;
        this.node = node;
        this.fileNames = fileNames;
        this.success = false;
    }

    @Override
    public void run() {
        try {
            String line;
            String fileNamesArg = StringUtils.join(fileNames, " ");
            Socket socket = ServerUtils.connectToServer(node.getIp() + ":" + 8789, 10);
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            dos.writeInt(HungryHippoServicesConstants.SCP_ACCESS);
            dos.flush();
            String status = dis.readUTF();
            if (!HungryHippoServicesConstants.SUCCESS.equals(status)) {
                this.countDownLatch.countDown();
                throw new RuntimeException("File Scp not possible for " + srcFolderPath);
            }

            logger.info("[{}] Lock acquired for {}", Thread.currentThread().getName(), srcFolderPath);

            Process process = Runtime.getRuntime().exec(commonCommandArg + " " + node.getIp() + " " + Thread.currentThread().getId() + " " + fileNamesArg);
            int processStatus = process.waitFor();

            if (processStatus != 0) {
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                while ((line = br.readLine()) != null) {
                    logger.error(line);
                }
                br.close();
                logger.error("[{}] Files failed for upload : {}", Thread.currentThread().getName(), fileNamesArg);
                success = false;
                this.countDownLatch.countDown();
                socket.close();
                throw new RuntimeException("File transfer failed for " + srcFolderPath);
            }
            dos.writeBoolean(true);
            socket.close();
            logger.info("[{}] Lock released for {}", Thread.currentThread().getName(), srcFolderPath);
            socket = ServerUtils.connectToServer(node.getIp() + ":" + 8789, 10);
            dataInputStreamMap.put(idx, new DataInputStream(socket.getInputStream()));
            dos = new DataOutputStream(socket.getOutputStream());
            dos.writeInt(HungryHippoServicesConstants.DATA_APPENDER);
            dos.writeUTF(remoteTargetFolder);
            dos.writeUTF(destinationPath);
            dos.flush();
            socketMap.put(idx, socket);
            success = true;
            this.countDownLatch.countDown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            success = false;
            this.countDownLatch.countDown();
            throw new RuntimeException("File transfer failed for " + srcFolderPath);
        }
    }

    public boolean isSuccess() {
        return success;
    }
}
