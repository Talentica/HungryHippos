package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.datareceiver.HHFileStatusCoordinator;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * Created by rajkishoreh on 12/1/17.
 */
public class MetaDataUpdaterService implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(MetaDataUpdaterService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public MetaDataUpdaterService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }


    @Override
    public void run() {
        logger.info("Updating metadata from {}", this.socket.getInetAddress());
        String hhFilePath = null;
        try {
            hhFilePath = dataInputStream.readUTF();
            String metadataFilePath = dataInputStream.readUTF();
            long metadatatFileSize = dataInputStream.readLong();
            File metadataFile = new File(metadataFilePath);
            if (!metadataFile.getParentFile().exists()) {
                metadataFile.getParentFile().mkdirs();
            }
            int bufferSize = 2048;
            byte[] buffer = new byte[bufferSize];
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(metadataFile, false));
            int len;
            long remainingDataSize = metadatatFileSize;
            while (remainingDataSize > 0) {
                len = dataInputStream.read(buffer);
                bos.write(buffer, 0, len);
                remainingDataSize -= len;
            }
            bos.flush();
            bos.close();
            this.dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            this.dataOutputStream.flush();
            logger.info("Metadata update completed from {}", this.socket.getInetAddress());
        } catch (IOException e) {
            e.printStackTrace();
            try {
                this.dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                this.dataOutputStream.flush();
                if (hhFilePath != null) {
                    HHFileStatusCoordinator.updateFailure(hhFilePath, e.getMessage());
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }


        } finally {
            try {
                this.socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
