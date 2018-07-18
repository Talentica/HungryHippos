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
package com.talentica.hungryHippos.node.service.orc;

import com.talentica.hungryHippos.node.joiners.orc.OrcFileMerger;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.FileSystemNotFoundException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class IncrementalOrcDataReceiver implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(IncrementalOrcDataReceiver.class);
    private Socket socket;

    public IncrementalOrcDataReceiver(Socket socket) {
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
                File destFile = new File(destinationPath);
                if (!destFile.exists()) destFile.mkdirs();
                String tmpPath = destinationPath + UUID.randomUUID().toString();
                try (FileOutputStream fileOutputStream = new FileOutputStream(tmpPath);
                     BufferedOutputStream os = new BufferedOutputStream(fileOutputStream)) {
                    int len;
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
                    os.flush();
                } catch (FileSystemNotFoundException e) {
                    logger.error("{} {}", destinationPath, e.toString());
                    e.printStackTrace();
                    throw e;
                }
                appendFileData(destinationPath, tmpPath);
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

    private void appendFileData(String destinationPath, String tmpPath) throws IOException {
        Path deltaFilePath = new Path(destinationPath + FileSystemConstants.ORC_DELTA_FILE_NAME);
        Path finalFilePath = new Path(destinationPath + FileSystemConstants.ORC_MAIN_FILE_NAME);
        List<Path> tmpList = new LinkedList<>();
        tmpList.add(new Path(tmpPath));
        if (new File(tmpPath).length() > OrcFileMerger.REWRITE_THRESHOLD) {
            OrcFileMerger.mergeFiles(finalFilePath, tmpList, true,true);
        } else {
            OrcFileMerger.mergeFiles(deltaFilePath, tmpList, true, true);
            if (new File(deltaFilePath.toString()).length() > OrcFileMerger.REWRITE_THRESHOLD) {
                List<Path> deltaList = new LinkedList<>();
                deltaList.add(deltaFilePath);
                Path intermediatePath = new Path(deltaFilePath.getParent().toString() + File.separator + UUID.randomUUID().toString());
                OrcFileMerger.rewriteData(intermediatePath, deltaList, true, true);
                List<Path> intermediateList = new LinkedList<>();
                intermediateList.add(intermediatePath);
                OrcFileMerger.mergeFiles(finalFilePath, intermediateList, true, true);
            }
        }
    }

}
