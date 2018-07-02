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

package com.talentica.hungryHippos.node.datareceiver.orc;

import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.node.datareceiver.SynchronousFolderDeleter;
import com.talentica.hungryHippos.storage.IncrementalDataEntity;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public class IncrementalOrcDataUploader implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(IncrementalOrcDataUploader.class);
    private Queue<IncrementalDataEntity> dataEntities;
    private String hostIp;
    private String port;
    private Map<String, Semaphore> semaphoreMap;

    public IncrementalOrcDataUploader(Queue<IncrementalDataEntity> dataEntities, String hostIp, String port, Map<String, Semaphore> semaphoreMap) {
        this.dataEntities = dataEntities;
        this.hostIp = hostIp;
        this.port = port;
        this.semaphoreMap = semaphoreMap;
    }

    @Override
    public Boolean call() throws Exception {
        String destPath = "";
        try {
            IncrementalDataEntity dataEntity = dataEntities.peek();
            if (dataEntity == null) {
                return true;
            }
            byte[] bytes = new byte[8192];

            try (Socket socket = ServerUtils.connectToServer(hostIp + ":" + port, 50);
                 DataInputStream dis = new DataInputStream(socket.getInputStream());
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                dos.writeInt(HungryHippoServicesConstants.ORC_INCREMENTAL_DATA_APPENDER);
                while (dataEntity != null) {
                    destPath = dataEntity.getDestPath();
                    Semaphore semaphore = semaphoreMap.get(destPath);
                    boolean sameFile = false;
                    if (semaphore.tryAcquire()) {
                        try {
                            sameFile = dataEntity ==  dataEntities.peek();
                            if (sameFile) {
                                dataEntity = dataEntities.poll();
                                dos.writeBoolean(true);
                                dos.writeUTF(destPath);
                                File file = new File(dataEntity.getSrcPath());
                                dos.writeLong(file.length());
                                try (FileInputStream fileInputStream = new FileInputStream(dataEntity.getSrcPath());
                                     BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
                                    int len;
                                    while ((len = bufferedInputStream.read(bytes)) > -1) {
                                        dos.write(bytes, 0, len);
                                    }
                                }
                                dos.flush();
                                boolean readSuccess = dis.readBoolean();
                            }

                        } finally {
                            semaphore.release();
                            if (sameFile) {
                                File file = new File(dataEntity.getSrcPath());
                                File parentFile = file.getParentFile();
                                dataEntity.updateComplete();
                                String[] siblings = parentFile.list();
                                if (siblings == null || siblings.length == 0) {
                                    SynchronousFolderDeleter.INSTANCE.deleteEmptyFolder(parentFile);
                                }
                            }
                        }
                    }
                    dataEntity = dataEntities.peek();
                }
                dos.writeBoolean(false);
                dos.flush();
                String status = dis.readUTF();
                if (!status.equals(HungryHippoServicesConstants.SUCCESS)) {
                    return false;
                }
            }
        } catch (Exception e) {
            logger.error("{} {} {}", destPath, hostIp, e.toString());
            e.printStackTrace();
            throw e;
        }

        return true;
    }

}
