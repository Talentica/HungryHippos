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
import com.talentica.hungryHippos.node.datareceiver.ApplicationCache;
import com.talentica.hungryHippos.node.datareceiver.SynchronousFolderDeleter;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.SecondStageZipFileDataStore;
import com.talentica.hungryHippos.storage.ResourceAllocator;
import com.talentica.hungryhippos.filesystem.FileStatistics;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 6/7/17.
 */
public class SecondStageNodeFileJoiner implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecondStageNodeFileJoiner.class);

    private volatile Queue<String> fileSrcQueue;

    private int fileId;

    private String hhFilePath;

    private static final int INT_OFFSET = Integer.BYTES;

    public SecondStageNodeFileJoiner(Queue<String> fileSrcQueue, String hhFilePath, int fileId) {
        this.fileSrcQueue = fileSrcQueue;
        this.hhFilePath = hhFilePath;
        this.fileId = fileId;
    }

    @Override
    public Boolean call() throws Exception {
        if (fileSrcQueue.isEmpty()) {
            return true;
        }
        try {
            ShardingApplicationContext context = ApplicationCache.INSTANCE.getContext(hhFilePath);
            FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
            int dataSize = dataDescription.getSize();
            String uniqueFolderName = FileSystemContext.getDataFilePrefix();
            Map<Integer, String> fileNames = ApplicationCache.INSTANCE.getIndexToFileNamesMap(hhFilePath, fileId);
            FileStatistics[] fileStatisticsArr = ApplicationCache.INSTANCE.getFileStatisticsMap(hhFilePath);
            SecondStageZipFileDataStore fileDataStore = new SecondStageZipFileDataStore(fileNames, ApplicationCache.INSTANCE.getMaxFiles(hhFilePath),
                    hhFilePath, uniqueFolderName,fileStatisticsArr);
            byte[] buf = new byte[INT_OFFSET + dataSize];
            ByteBuffer indexBuffer = ByteBuffer.wrap(buf);
            ByteBuffer dataBuffer = ByteBuffer.wrap(buf,INT_OFFSET,dataSize);
            HHRowReader hhRowReader = new HHRowReader(dataDescription,dataBuffer,INT_OFFSET);

            int cols = dataDescription.getNumberOfDataFields();
            int index;
            String srcFileName;
            while ((srcFileName = fileSrcQueue.poll()) != null) {
                File srcFile = new File(srcFileName);

                if (!fileDataStore.isUsingBufferStream()) {
                    if (ResourceAllocator.INSTANCE.isMemoryAvailableForBuffer(fileNames.size())) {
                        LOGGER.info("Upgrading store for {}", srcFileName);
                        fileDataStore.upgradeStreams();
                    }
                }
                try {
                    FileInputStream fis = new FileInputStream(srcFile);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    while (bis.read(buf) != -1) {
                        index = indexBuffer.getInt(0);
                        FileStatistics fileStatistics = fileStatisticsArr[index];
                        for (int i = 0; i < cols; i++) {
                            fileStatistics.updateColumnStatistics(i,hhRowReader.readAtColumn(i));
                        }
                        fileDataStore.storeRow(index, buf, INT_OFFSET, dataSize);
                    }
                    bis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
                srcFile.delete();
                SynchronousFolderDeleter.INSTANCE.deleteEmptyFolder(srcFile.getParentFile());
            }
            fileDataStore.sync();

        } catch (Exception e) {
            LOGGER.error("Processing for {} fileId {}", hhFilePath, fileId);
            e.printStackTrace();
            throw e;
        } finally {
            ApplicationCache.INSTANCE.releaseContext(hhFilePath);
        }
        return true;
    }
}
