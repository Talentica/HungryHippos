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

package com.talentica.hungryHippos.node.joiners;

import com.talentica.hungryHippos.node.datareceiver.SynchronousFolderDeleter;
import com.talentica.hungryHippos.storage.IncrementalDataEntity;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

public class SnappyFileAppender implements Callable<Boolean> {
    private static final Logger logger = LoggerFactory.getLogger(SnappyFileAppender.class);
    private Queue<IncrementalDataEntity> dataEntities;
    private Semaphore semaphore;


    public SnappyFileAppender(Queue<IncrementalDataEntity> dataEntities, Semaphore semaphore) {
        this.dataEntities = dataEntities;
        this.semaphore = semaphore;
    }

    @Override
    public Boolean call() throws Exception {
        if (!semaphore.tryAcquire()) {
            return true;
        }
        try {
            IncrementalDataEntity dataEntity = dataEntities.poll();
            if (dataEntity == null) {
                return true;
            }

            String destPath = dataEntity.getDestPath();

            //logger.info("Appending for {}", destPath);
            byte[] bytes = new byte[8192];
            try (
                    FileOutputStream fileOutputStream = new FileOutputStream(destPath, true);
                    SnappyOutputStream os = new SnappyOutputStream(fileOutputStream,FileSystemConstants.SNAPPY_BLOCK_SIZE);
            ) {
                while (dataEntity != null) {
                    try (FileInputStream fis = new FileInputStream(dataEntity.getSrcPath());
                         BufferedInputStream bis = new BufferedInputStream(fis)) {
                        int len;
                        while ((len = bis.read(bytes)) > -1) {
                            os.write(bytes, 0, len);
                        }
                    } finally {
                        dataEntity.updateComplete();
                        File parentFile = new File(dataEntity.getSrcPath()).getParentFile();
                        String[] children = parentFile.list();
                        if (children == null || children.length == 0) {
                            SynchronousFolderDeleter.INSTANCE.deleteEmptyFolder(parentFile);
                        }
                    }
                    dataEntity = dataEntities.poll();
                }
                os.flush();
            } catch (Exception e) {
                logger.error("{} {}", destPath, e.toString());
                e.printStackTrace();
                throw e;
            }
            //logger.info("Completed appending for {}", destPath);
        } finally {
            semaphore.release();
        }

        return true;
    }

}
