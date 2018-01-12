/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */
package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.datareceiver.HHFileStatusCoordinator;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * Created by rajkishoreh on 12/1/17.
 */
public class TarDataUpdaterService implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(TarDataUpdaterService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public TarDataUpdaterService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }


    @Override
    public void run() {
        logger.info("Updating data from {}", this.socket.getInetAddress());
        String hhFilePath = null;
        try {
            hhFilePath = dataInputStream.readUTF();
            String dataFolderPath = dataInputStream.readUTF();
            String tarFilePath = dataInputStream.readUTF();
            long datatFileSize = dataInputStream.readLong();
            File tarFile = new File(tarFilePath);
            int bufferSize = 2048;
            byte[] buffer = new byte[bufferSize];
            try (FileOutputStream fos = new FileOutputStream(tarFile, false);
                 BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                int len;
                long remainingDataSize = datatFileSize;
                while (remainingDataSize > 0) {
                    len = dataInputStream.read(buffer);
                    bos.write(buffer, 0, len);
                    remainingDataSize -= len;
                }
                bos.flush();
                fos.flush();
            }
            FileUtils.deleteQuietly(new File(dataFolderPath));
            TarAndGzip.untarTGzFile(tarFilePath);
            tarFile.delete();
            this.dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            this.dataOutputStream.flush();
            logger.info("data update completed from {}", this.socket.getInetAddress());
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
