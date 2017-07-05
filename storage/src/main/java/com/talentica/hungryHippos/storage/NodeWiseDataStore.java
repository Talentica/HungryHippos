/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.storage;

import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rajkishoreh on 15/5/17.
 */
public class NodeWiseDataStore implements DataStore {

    private static final Logger logger = LoggerFactory.getLogger(NodeWiseDataStore.class);
    private FileOutputStream[][] fileOutputStreams;
    private BufferedOutputStream[][] bufferedOutputStreams;
    private String hungryHippoFilePath;
    private String dataFilePrefix;
    private int numDimensions;
    private Map<Integer, FileOutputStream> nodeIdFileOutputStreamMap;
    private Map<Integer, BufferedOutputStream> nodeIdBufferOutputStreamMap;
    private ByteBuffer indexBuf;
    private byte[] indexBytes;
    private boolean usingBufferStream;


    public NodeWiseDataStore(Map<Integer, String> fileNames, Map<String, int[]> fileToNodeMap, int maxBucketSize, int numDimensions,
                             String hungryHippoFilePath, String fileName, int noOfNodes) throws IOException {
        this.nodeIdFileOutputStreamMap = new HashMap<>();
        this.nodeIdBufferOutputStreamMap = new HashMap<>();
        this.hungryHippoFilePath = hungryHippoFilePath;
        this.dataFilePrefix = FileSystemContext.getRootDirectory() + hungryHippoFilePath
                + File.separator + fileName;
        int maxFiles = (int) Math.pow(maxBucketSize, numDimensions);
        File file = new File(dataFilePrefix);
        if (!file.exists()) {
            boolean flag = file.mkdirs();
            if (flag) {
                logger.info("created data folder");
            } else {
                logger.info("Not able to create dataFolder");
            }
        }
        indexBytes = new byte[4];
        indexBuf = ByteBuffer.wrap(indexBytes);
        dataFilePrefix = dataFilePrefix + "/";
        this.numDimensions = numDimensions;
        bufferedOutputStreams = new BufferedOutputStream[maxFiles][numDimensions];
        fileOutputStreams = new FileOutputStream[maxFiles][numDimensions];
        usingBufferStream = ResourceAllocator.INSTANCE.allocateResources(fileNames, fileToNodeMap, nodeIdFileOutputStreamMap, nodeIdBufferOutputStreamMap, this.fileOutputStreams,
                this.bufferedOutputStreams, this.dataFilePrefix, numDimensions, noOfNodes, maxFiles);
        if(usingBufferStream){
            fileOutputStreams = null;
        }else{
            bufferedOutputStreams = null;
        }
    }

    @Override
    public boolean storeRow(String name, byte[] raw) {
        return false;
    }

    @Override
    public void storeRow(int index, byte[] raw) {
        try {
            indexBuf.putInt(0,index);
            if (usingBufferStream) {
                for (int i = 0; i < numDimensions; i++) {
                    bufferedOutputStreams[index][i].write(indexBytes);
                    bufferedOutputStreams[index][i].write(raw);
                }
            } else {
                for (int i = 0; i < numDimensions; i++) {
                    fileOutputStreams[index][i].write(indexBytes);
                    fileOutputStreams[index][i].write(raw);
                }
            }
        } catch (IOException e) {
            logger.error("Error occurred while writing data received to datastore. {} ", e.toString());
            e.printStackTrace();
        }

    }

    @Override
    public void sync() {

        for (Map.Entry<Integer, FileOutputStream> nameStreamEntry : nodeIdFileOutputStreamMap.entrySet()) {
            try {
                if(usingBufferStream){
                    nodeIdBufferOutputStreamMap.get(nameStreamEntry.getKey()).flush();
                }
                nameStreamEntry.getValue().flush();
            } catch (IOException e) {
                logger.error("Error occurred while flushing " + nameStreamEntry.getKey() + " output stream. {}", e);
            } finally {
                try {
                    if (nameStreamEntry.getValue() != null) {
                        if(usingBufferStream){
                            nodeIdBufferOutputStreamMap.get(nameStreamEntry.getKey()).close();
                        }
                        nameStreamEntry.getValue().close();
                    }
                } catch (IOException e) {
                    logger.warn("\n\tUnable to close the connection; exception :: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public String getHungryHippoFilePath() {
        return hungryHippoFilePath;
    }

    @Override
    public void reset() {

    }


}
