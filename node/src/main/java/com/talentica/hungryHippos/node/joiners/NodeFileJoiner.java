/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.node.joiners;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.datareceiver.ShardingResourceCache;
import com.talentica.hungryHippos.node.datareceiver.SynchronousFolderDeleter;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.ResourceAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 17/5/17.
 */
public class NodeFileJoiner implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeFileJoiner.class);

    private volatile Queue<String> fileSrcQueue;

    private String hhFilePath;

    public NodeFileJoiner(Queue<String> fileSrcQueue, String hhFilePath) {
        this.fileSrcQueue = fileSrcQueue;
        this.hhFilePath = hhFilePath;
    }

    @Override
    public Boolean call() throws Exception {
        if (fileSrcQueue.isEmpty()) {
            return true;
        }
        try {
            ShardingApplicationContext context = ShardingResourceCache.INSTANCE.getContext(hhFilePath);
            FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
            int offset = 4;
            int dataSize = dataDescription.getSize();
            NodeFileMapper nodeFileMapper = new NodeFileMapper(hhFilePath);
            int index;
            String srcFileName;
            while ((srcFileName = fileSrcQueue.poll()) != null) {
                byte[] buf = new byte[offset + dataSize];
                ByteBuffer indexBuffer = ByteBuffer.wrap(buf);
                File srcFile = new File(srcFileName);
                File srcFolder = new File(srcFile.getParent());
                if (!nodeFileMapper.isUsingBufferStream()) {
                    System.gc();
                    if (ResourceAllocator.INSTANCE.isMemoryAvailableForBuffer(nodeFileMapper.getNoOfFiles())) {
                        LOGGER.info("Upgrading store for {}", srcFileName);
                        nodeFileMapper.upgradeStore();
                    }
                }
                try {
                    LOGGER.info("Processing file {}", srcFileName);
                    if (srcFile.exists()) {
                        FileInputStream fis = new FileInputStream(srcFile);
                        BufferedInputStream bis = new BufferedInputStream(fis);
                        while ((bis.read(buf)) != -1) {
                            index = indexBuffer.getInt(0);
                            nodeFileMapper.storeRow(index, buf, offset, dataSize);
                        }
                        bis.close();
                        srcFile.delete();
                        SynchronousFolderDeleter.INSTANCE.deleteEmptyFolder(srcFolder);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }

            }
            nodeFileMapper.sync();
        } finally {
            ShardingResourceCache.INSTANCE.releaseContext(hhFilePath);
        }
        return true;
    }
}
