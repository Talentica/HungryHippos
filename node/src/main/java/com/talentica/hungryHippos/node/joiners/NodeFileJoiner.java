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
import com.talentica.hungryHippos.node.datareceiver.IncrementalDataHandler;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.FileStatistics;
import com.talentica.hungryHippos.node.datareceiver.ShardingResourceCache;
import com.talentica.hungryHippos.node.datareceiver.SynchronousFolderDeleter;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.ResourceAllocator;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.*;
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
        String srcFileName = fileSrcQueue.poll();
        if (srcFileName==null) {
            return true;
        }
        try {
            ShardingApplicationContext context = ShardingResourceCache.INSTANCE.getContext(hhFilePath);
            FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
            int offset = Integer.BYTES;
            int dataSize = dataDescription.getSize();
            Map<Integer, String> indexToFileNamesV2 = ShardingResourceCache.INSTANCE.getIndexToFileNamesForFirstDimension(hhFilePath);
            FirstDimensionNodeFileMapper nodeFileMapper = new FirstDimensionNodeFileMapper(hhFilePath,indexToFileNamesV2);
            int index;

            byte[] buf = new byte[offset + dataSize];
            ByteBuffer indexBuffer = ByteBuffer.wrap(buf,0,offset);
            ByteBuffer dataBuffer = ByteBuffer.wrap(buf,offset,dataSize);
            HHRowReader hhRowReader = new HHRowReader(dataDescription,dataBuffer,offset);
            FileStatistics[] fileStatisticsArr = ShardingResourceCache.INSTANCE.getFileStatisticsMap(hhFilePath);
            while (srcFileName != null) {
                int cols = dataDescription.getNumberOfDataFields();
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
                            FileStatistics fileStatistics = fileStatisticsArr[index];
                            for (int i = 0; i < cols; i++) {
                                fileStatistics.updateColumnStatistics(i,hhRowReader.readAtColumn(i));
                            }
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
                srcFileName = fileSrcQueue.poll();
                while(srcFileName==null&&!IncrementalDataHandler.INSTANCE.checkAvailable()){
                    Thread.sleep(5000);
                    srcFileName = fileSrcQueue.poll();
                }
            }
            nodeFileMapper.sync();

            String srcFolderPath = FileSystemContext.getRootDirectory() + hhFilePath
                    + File.separator + nodeFileMapper.getUniqueFolderName()+File.separator;
            String destFolderPath = FileSystemContext.getRootDirectory() + hhFilePath
                    + File.separator + FileSystemContext.getDataFilePrefix()+File.separator;
            Map<String, int[]>  fileToNodeMap = ShardingResourceCache.INSTANCE.getHHFileNamesCalculator(hhFilePath).getFileToNodeMap();
            IncrementalDataHandler.INSTANCE.initialize(hhFilePath, destFolderPath, indexToFileNamesV2.values(),fileToNodeMap);
            try{
                for(String fileName:indexToFileNamesV2.values()){
                    String srcPath = srcFolderPath+fileName;
                    File srcFile = new File(srcPath);
                    if(srcFile.length()>0){
                        IncrementalDataHandler.INSTANCE.register(hhFilePath,srcPath,destFolderPath+fileName+ FileSystemConstants.ZIP_EXTENSION,fileToNodeMap.get(fileName));
                    }else{
                        srcFile.delete();
                    }
                }
            }finally {
                IncrementalDataHandler.INSTANCE.completedRegister(hhFilePath);
            }
            SynchronousFolderDeleter.INSTANCE.deleteEmptyFolder(new File(srcFolderPath));

        } finally {
            ShardingResourceCache.INSTANCE.releaseContext(hhFilePath);
        }
        return true;
    }
}
