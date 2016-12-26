package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.datareceiver.FileJoiner;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by rajkishoreh on 20/12/16.
 */
public class DataAppenderService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(DataAppenderService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public DataAppenderService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        File srcFolder = null;
        String srcFolderPath = null;
        try {
            srcFolderPath = dataInputStream.readUTF();
            srcFolder = new File(srcFolderPath);
            String destFolderPath = dataInputStream.readUTF();
            logger.info("[{}] joining {} into {}",Thread.currentThread().getName(),srcFolderPath,destFolderPath);
            String lockString = destFolderPath+socket.getInetAddress();
            if (srcFolder.exists()) {
                FileJoiner.join(srcFolderPath, destFolderPath, lockString);
                dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
                logger.info("[{}] Successfully joined {} into {}",Thread.currentThread().getName(),srcFolderPath,destFolderPath);

            }else{
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                logger.info("[{}] sourcefolder  {} not found",Thread.currentThread().getName(),srcFolderPath);
            }
            dataOutputStream.flush();

        } catch (IOException | InterruptedException e) {
            try {
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                dataOutputStream.flush();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (srcFolder != null) {
                    if (srcFolder.exists()) {
                        logger.info("[{}] Deleting {} ",Thread.currentThread().getName(),srcFolderPath);
                        FileUtils.deleteDirectory(srcFolder);
                    }
                }
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
