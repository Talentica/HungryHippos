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

import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.net.Socket;
import java.nio.file.*;

public class IncrementalDataReceiver implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(IncrementalDataReceiver.class);
    private Socket socket;

    public IncrementalDataReceiver(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        DataInputStream dis = null;
        DataOutputStream dos = null;
        try {
            dis = new DataInputStream(this.socket.getInputStream());
            dos = new DataOutputStream(this.socket.getOutputStream());
            byte[] bytes = new byte[8192];
            while (dis.readBoolean()) {
                String destinationPath = dis.readUTF();
                try (FileOutputStream fileOutputStream = new FileOutputStream(destinationPath,true);
                     SnappyOutputStream os = new SnappyOutputStream(fileOutputStream,FileSystemConstants.SNAPPY_BLOCK_SIZE)) {
                    int len;
                    do {
                        long size = dis.readLong();
                        while (size > 0) {
                            len = dis.read(bytes);
                            try {
                                os.write(bytes, 0, len);
                            } catch (ClosedFileSystemException e) {
                                logger.error("{} {}", destinationPath, e.toString());
                                e.printStackTrace();
                            }
                            size -= len;
                        }
                        dos.writeBoolean(true);
                        dos.flush();
                    }while (dis.readBoolean());
                    os.flush();
                }catch (FileSystemNotFoundException e){
                    logger.error("{} {}",destinationPath,e.toString());
                    e.printStackTrace();
                    throw e;
                }
                dos.writeBoolean(true);
                dos.flush();
            }
            dos.writeUTF(HungryHippoServicesConstants.SUCCESS);
            dos.flush();
        } catch (IOException e) {
            logger.error(e.toString());
            try {
                if (dos != null) {
                    dos.writeUTF(HungryHippoServicesConstants.FAILURE);
                    dos.flush();
                }
            } catch (IOException e1) {
                logger.error(e1.toString());
            }

        } finally {
            if (this.socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error(e.toString());
                }
            }
        }
    }

}
