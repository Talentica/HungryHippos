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

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.datareceiver.ShardingResourceCache;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.FileDataStoreFirstStage;
import com.talentica.hungryHippos.storage.ResourceAllocator;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 17/5/17.
 */
public class FirstStageNodeFileJoiner implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirstStageNodeFileJoiner.class);

    private volatile Queue<String> fileSrcQueue;

    private String hhFilePath;

    private static final int INT_OFFSET = 4;

    public FirstStageNodeFileJoiner(Queue<String> fileSrcQueue, String hhFilePath) {
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
            int dataSize = dataDescription.getSize();
            String uniqueFolderName = UUID.randomUUID().toString();
            Map<Integer, String> fileNames = ShardingResourceCache.INSTANCE.getIndexToFileNamesMap(hhFilePath);
            FileDataStoreFirstStage fileDataStoreFirstStage = new FileDataStoreFirstStage(fileNames, hhFilePath, true, uniqueFolderName,
                    ShardingResourceCache.INSTANCE.getMaxFiles(hhFilePath), ShardingResourceCache.INSTANCE.getReduceFactor(hhFilePath));
            int index;
            String srcFileName;
            int bufLen = INT_OFFSET + dataSize;
            byte[] buf = new byte[bufLen];
            ByteBuffer indexBuffer = ByteBuffer.wrap(buf);
            while ((srcFileName = fileSrcQueue.poll()) != null) {
                File srcFile = new File(srcFileName);
                if (!fileDataStoreFirstStage.isUsingBufferStream()) {
                    if (ResourceAllocator.INSTANCE.isMemoryAvailableForBuffer(fileNames.size())) {
                        LOGGER.info("Upgrading store for {}", srcFileName);
                        fileDataStoreFirstStage.upgradeStreams();
                    }
                }
                try {
                    FileInputStream fis = new FileInputStream(srcFile);
                    BufferedInputStream bis = new BufferedInputStream(fis,10485760);
                    while (bis.read(buf) != -1) {
                        index = indexBuffer.getInt(0);
                        fileDataStoreFirstStage.storeRow(index, buf);
                    }
                    bis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
                srcFile.delete();

                File srcFolder = srcFile.getParentFile();
                String[] children = srcFolder.list();
                if (children == null || children.length == 0) {
                    FileUtils.deleteQuietly(srcFolder);
                }
            }

            fileDataStoreFirstStage.sync();
            String firstStageOutputPath = FileSystemContext.getRootDirectory() + hhFilePath + File.separator + uniqueFolderName;
            File firstStageOutputFolder = new File(firstStageOutputPath);
            File[] files = firstStageOutputFolder.listFiles();
            SecondStageNodeFileJoinerCaller.INSTANCE.addFiles(firstStageOutputPath, hhFilePath, files);
        } finally {
            ShardingResourceCache.INSTANCE.releaseContext(hhFilePath);
        }

        return true;
    }
}
