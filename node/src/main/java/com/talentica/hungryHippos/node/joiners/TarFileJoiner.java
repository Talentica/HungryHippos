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

import com.talentica.hungryHippos.node.datareceiver.ApplicationCache;
import com.talentica.hungryHippos.storage.ResourceAllocator;
import com.talentica.hungryHippos.utility.scp.TarAndUntar;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 19/5/17.
 */
public class TarFileJoiner implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TarFileJoiner.class);

    private volatile Queue<String> fileSrcQueue;

    private String hhFilePath;

    private UnTarStrategy unTarStrategy;

    public TarFileJoiner(Queue<String> fileSrcQueue, String hhFilePath, UnTarStrategy unTarStrategy) {
        this.fileSrcQueue = fileSrcQueue;
        this.hhFilePath = hhFilePath;
        this.unTarStrategy = unTarStrategy;
    }

    @Override
    public Boolean call() throws Exception {
        if (fileSrcQueue.isEmpty()) {
            return true;
        }
        if (unTarStrategy == UnTarStrategy.UNTAR_ON_CONTINUOUS_STREAMS) {
            untarOnContinuousStreams();
        } else {
            untarOneFileAtATime();
        }
        return true;
    }

    private void untarOneFileAtATime() throws IOException {
        String destFolderPath = FileSystemContext.getRootDirectory() + hhFilePath + File.separator + FileSystemContext.getDataFilePrefix();
        String srcFileName;
        while ((srcFileName = fileSrcQueue.poll()) != null) {
            System.gc();
            File srcTarFile = new File(srcFileName);
            File srcFolder = srcTarFile.getParentFile();
            LOGGER.info("Processing file {}", srcFileName);
            try {
                TarAndUntar.untarAndAppend(srcFileName, destFolderPath);
            } catch (FileNotFoundException e) {
                LOGGER.error("[{}] Retrying File untar for {}", Thread.currentThread().getName(), srcFileName);
            }
            srcTarFile.delete();
            FileUtils.deleteDirectory(srcFolder);
        }
    }

    private void untarOnContinuousStreams() throws JAXBException, InterruptedException, ClassNotFoundException, KeeperException, IOException {
        try {
            ApplicationCache.INSTANCE.getContext(hhFilePath);
            NodeFileMapper nodeFileMapper = new NodeFileMapper(hhFilePath);
            String srcFileName;
            while ((srcFileName = fileSrcQueue.poll()) != null) {
                System.gc();
                File srcFile = new File(srcFileName);
                File srcFolder = new File(srcFile.getParent());
                if (!nodeFileMapper.isUsingBufferStream()) {
                    if (ResourceAllocator.INSTANCE.isMemoryAvailableForBuffer(nodeFileMapper.getNoOfFiles())) {
                        LOGGER.info("Upgrading store for {}", srcFileName);
                        nodeFileMapper.upgradeStore();
                    }
                }
                try {
                    LOGGER.info("Processing file {}", srcFileName);
                    if (srcFile.exists()) {
                        if (nodeFileMapper.isUsingBufferStream()) {
                            TarAndUntar.untarToStream(srcFileName, nodeFileMapper.getFileNameToBufferedOutputStreamMap());
                        } else {
                            TarAndUntar.untarToStream(srcFileName, nodeFileMapper.getFileNameToOutputStreamMap());
                        }
                        srcFile.delete();
                        FileUtils.deleteDirectory(srcFolder);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
            nodeFileMapper.sync();
        } finally {
            ApplicationCache.INSTANCE.releaseContext(hhFilePath);
        }
    }
}
