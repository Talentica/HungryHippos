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
    private String hhFilePath;


    public FileUploader(CountDownLatch countDownLatch, String srcFolderPath, String destinationPath, String remoteTargetFolder, String commonCommandArg, int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Node node, Set<String> fileNames, String hhFilePath) {
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
        this.hhFilePath = hhFilePath;
    }

    @Override
    public void run() {
        try {
            String line;
            String fileNamesArg = StringUtils.join(fileNames, " ");
            logger.info("[{}] File Upload started for {} to {}", Thread.currentThread().getName(), srcFolderPath,node.getIp());

            int processStatus = -1;
            int noOfRemainingAttempts =25;
            String tarFileName = Thread.currentThread().getId()+".tar";
            while(noOfRemainingAttempts>0&&processStatus<0){
            Process tarProcess = Runtime.getRuntime().exec(commonCommandArg + " " + tarFileName + " " + fileNamesArg);
            processStatus = tarProcess.waitFor();

            if (processStatus != 0) {
                BufferedReader br = new BufferedReader(new InputStreamReader(tarProcess.getErrorStream()));
                while ((line = br.readLine()) != null) {
                    logger.error(line);
                }
                br.close();
            }
            logger.error("[{}] Retrying File tar for {} after 5 seconds", Thread.currentThread().getName(), srcFolderPath);
            noOfRemainingAttempts--;
            Thread.sleep(5000);            
            }
            if(processStatus<0){
                logger.error("[{}] Files failed for tar : {}", Thread.currentThread().getName(), fileNamesArg);
                success = false;
                this.countDownLatch.countDown();
                throw new RuntimeException("File transfer failed for " + srcFolderPath);
            }
            logger.info("[{}] Lock released for {}", Thread.currentThread().getName(), srcFolderPath);
            Socket socket = ServerUtils.connectToServer(node.getIp() + ":" + 8789, 50);
            dataInputStreamMap.put(idx, new DataInputStream(socket.getInputStream()));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeInt(HungryHippoServicesConstants.DATA_APPENDER);
            dos.writeUTF(hhFilePath);
            dos.writeUTF(remoteTargetFolder);
            dos.writeUTF(tarFileName);
            dos.writeUTF(destinationPath);
            dos.flush();
            File srcFile = new File(srcFolderPath+File.separator+tarFileName);
            dos.writeLong(srcFile.length());
            int bufferSize = 2048;
            byte[] buffer = new byte[bufferSize];
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(srcFile),10*bufferSize);
            int len;
            while((len=bis.read(buffer))>-1) {
                dos.write(buffer, 0, len);
            }
            dos.flush();
            bis.close();
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
