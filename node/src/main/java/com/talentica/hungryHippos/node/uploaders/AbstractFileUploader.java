package com.talentica.hungryHippos.node.uploaders;

import com.talentica.hungryHippos.coordination.server.ServerUtils;
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
 * Created by rajkishoreh on 28/4/17.
 */
public abstract class AbstractFileUploader implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(AbstractFileUploader.class);
    private CountDownLatch countDownLatch;
    private String srcFolderPath, destinationPath;
    private int idx;
    private Map<Integer, DataInputStream> dataInputStreamMap;
    private Map<Integer, Socket> socketMap;
    private Node node;
    private Set<String> fileNames;
    private boolean success;
    private String hhFilePath;
    private String tarFileName;


    public AbstractFileUploader(CountDownLatch countDownLatch, String srcFolderPath, String destinationPath,
                        int idx,Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Node node,
                        Set<String> fileNames, String hhFilePath, String tarFilename) {
        this.countDownLatch = countDownLatch;
        this.srcFolderPath = srcFolderPath;
        this.destinationPath = destinationPath;
        this.idx = idx;
        this.dataInputStreamMap = dataInputStreamMap;
        this.socketMap = socketMap;
        this.node = node;
        this.fileNames = fileNames;
        this.success = false;
        this.hhFilePath = hhFilePath;
        this.tarFileName = tarFilename;
        logger.info("Instance Initialized");
    }

    @Override
    public void run() {
        File srcFile = new File(srcFolderPath + File.separator + tarFileName);
        try {
            logger.info("[{}] File Upload started for {} to {}", Thread.currentThread().getName(),
                    srcFolderPath, node.getIp());
            generateTarFile(srcFile);
            logger.info("[{}] Tar file generated for {}", Thread.currentThread().getName(), srcFolderPath);
            sendTarFile(srcFile);
            success = true;
            this.countDownLatch.countDown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            success = false;
            this.countDownLatch.countDown();
            if (!(new File(srcFolderPath)).exists()) {
                logger.error("[{}] Source folder {} does not exist", Thread.currentThread().getName(),
                        srcFolderPath);
            }
            throw new RuntimeException(
                    "File transfer failed for " + srcFolderPath + " to " + node.getIp());
        } finally {
            if (srcFile.exists()) {
                srcFile.delete();
            }
        }
    }

    protected void generateTarFile(File srcFile) {
        String fileNamesArg = StringUtils.join(fileNames, " ");
        int noOfRemainingAttempts = 25;
        while(noOfRemainingAttempts > 0 && !srcFile.exists()){
            try{
                createTar(tarFileName);
                break;
            }catch(IOException e){
                noOfRemainingAttempts--;
                logger.error("[{}] Retrying File tar for {}",
                        Thread.currentThread().getName(), srcFolderPath);
                e.printStackTrace();
            }
        }
        if(noOfRemainingAttempts == 0 || !srcFile.exists()){
            logger.error("[{}] Files failed for tar : {}", Thread.currentThread().getName(),
                    fileNamesArg);
            success = false;
            this.countDownLatch.countDown();
            throw new RuntimeException(
                    "File transfer failed for " + srcFolderPath + " to " + node.getIp());
        }
    }

    protected abstract void createTar(String tarFilename) throws IOException;


    private void sendTarFile(File srcFile) throws IOException, InterruptedException {
        Socket socket = ServerUtils.connectToServer(node.getIp() + ":" + node.getPort(), 50);
        dataInputStreamMap.put(idx, new DataInputStream(socket.getInputStream()));
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        writeAppenderType(dos);
        dos.writeUTF(hhFilePath);
        dos.writeUTF(tarFileName);
        dos.writeUTF(destinationPath);
        dos.writeLong(srcFile.length());
        dos.flush();
        int bufferSize = 2048;
        byte[] buffer = new byte[bufferSize];
        BufferedInputStream bis =
                new BufferedInputStream(new FileInputStream(srcFile), 10 * bufferSize);
        int len;
        while ((len = bis.read(buffer)) > -1) {
            dos.write(buffer, 0, len);
        }
        dos.flush();
        bis.close();
        socketMap.put(idx, socket);
    }

    abstract public void writeAppenderType(DataOutputStream dos) throws IOException ;

    public boolean isSuccess() {
        return success;
    }
}
