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
package com.talentica.hungryHippos.node.joiners.orc;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.datareceiver.ApplicationCache;
import com.talentica.hungryHippos.node.datareceiver.IncrementalDataHandler;
import com.talentica.hungryHippos.node.datareceiver.SynchronousFolderDeleter;
import com.talentica.hungryHippos.node.joiners.HHRowReader;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
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
 * Created by rajkishoreh on 7/5/18.
 */
public class NodeFileJoinerORC implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeFileJoinerORC.class);

    private volatile Queue<String> fileSrcQueue;

    private String hhFilePath;

    public NodeFileJoinerORC(Queue<String> fileSrcQueue, String hhFilePath) {
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
            ShardingApplicationContext context = ApplicationCache.INSTANCE.getContext(hhFilePath);
            FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
            int offset = Integer.BYTES;
            int dataSize = dataDescription.getSize();
            Map<Integer, String> indexToFileNamesV2 = ApplicationCache.INSTANCE.getIndexToFileNamesForFirstDimension(hhFilePath);
            OrcNodeFileMapper orcNodeFileMapper = new OrcNodeFileMapper(hhFilePath,indexToFileNamesV2);
            int index;

            byte[] buf = new byte[offset + dataSize];
            ByteBuffer indexBuffer = ByteBuffer.wrap(buf,0,offset);
            ByteBuffer dataBuffer = ByteBuffer.wrap(buf,offset,dataSize);
            HHRowReader hhRowReader = new HHRowReader(dataDescription,dataBuffer,offset);

            while (srcFileName != null) {
                int cols = dataDescription.getNumberOfDataFields();
                Object[] objects = new Object[cols];
                File srcFile = new File(srcFileName);
                File srcFolder = new File(srcFile.getParent());
                try {
                    LOGGER.info("Processing file {}", srcFileName);
                    if (srcFile.exists()) {
                        FileInputStream fis = new FileInputStream(srcFile);
                        BufferedInputStream bis = new BufferedInputStream(fis);
                        while ((bis.read(buf)) != -1) {
                            index = indexBuffer.getInt(0);
                            for (int i = 0; i < cols; i++) {
                                objects[i] = hhRowReader.readAtColumn(i);
                            }
                            orcNodeFileMapper.storeRow(index, objects);
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
            orcNodeFileMapper.sync();

            String srcFolderPath = FileSystemContext.getRootDirectory() + hhFilePath
                    + File.separator + orcNodeFileMapper.getUniqueFolderName()+File.separator;
            String destFolderPath = FileSystemContext.getRootDirectory() + hhFilePath
                    + File.separator + FileSystemContext.getDataFilePrefix()+File.separator;
            Map<String, int[]>  fileToNodeMap = ApplicationCache.INSTANCE.getFiletoNodeMap(hhFilePath);
            IncrementalDataHandler.INSTANCE.initialize(hhFilePath, srcFolderPath,destFolderPath, indexToFileNamesV2.values(),fileToNodeMap);
            try{
                for(String fileName:indexToFileNamesV2.values()){
                    String srcPath = srcFolderPath+fileName;
                    File srcFile = new File(srcPath);
                    if(srcFile.length()>0){
                        IncrementalDataHandler.INSTANCE.register(hhFilePath,srcPath,destFolderPath+fileName+ File.separator,fileToNodeMap.get(fileName));
                    }else{
                        srcFile.delete();
                    }
                }
            }finally {
                IncrementalDataHandler.INSTANCE.completedRegister(hhFilePath);
            }
            SynchronousFolderDeleter.INSTANCE.deleteEmptyFolder(new File(srcFolderPath));

        } finally {
            ApplicationCache.INSTANCE.releaseContext(hhFilePath);
        }
        return true;
    }
}
