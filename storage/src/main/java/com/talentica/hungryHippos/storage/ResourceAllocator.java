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

import com.talentica.hungryHippos.utility.MemoryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * Created by rajkishoreh on 18/5/17.
 */
public enum ResourceAllocator {
    INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(ResourceAllocator.class);

    public synchronized boolean allocateResources(Map<Integer, String> fileNames, OutputStream[] outputStreams,
                                                  String dataFilePrefix, Map<String, FileOutputStream> fileNameToOutputStreamMap,Map<String, BufferedOutputStream> fileNameToBufferedOutputStreamMap,
                                                  boolean append, boolean reqForUpgrade, FileDataStore fileDataStore) throws FileNotFoundException {
        boolean usingBufferStream = true;
        long usableMemory = MemoryStatus.getUsableMemory();
        long memoryRequiredForBufferedStream = fileNames.size() * 1024;
        if (usableMemory > memoryRequiredForBufferedStream) {
            if(reqForUpgrade) {
                fileDataStore.sync();
            }
            for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                FileOutputStream fos = new FileOutputStream(dataFilePrefix + entry.getValue(), append);
                BufferedOutputStream bos = new BufferedOutputStream(fos, 1024);
                outputStreams[entry.getKey()] = bos;
                fileNameToOutputStreamMap.put(entry.getValue(), fos);
                fileNameToBufferedOutputStreamMap.put(entry.getValue(),bos);
            }
        } else {
            usingBufferStream = false;
            if(reqForUpgrade){
                return usingBufferStream;
            }
            for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                FileOutputStream fos = new FileOutputStream(dataFilePrefix + entry.getValue(), append);
                outputStreams[entry.getKey()] = fos;
                fileNameToOutputStreamMap.put(entry.getValue(), fos);
            }
        }
        logger.info("{} resources allocated for {}", fileNames.size(), dataFilePrefix);
        return usingBufferStream;
    }


    public synchronized boolean allocateResources(Map<Integer, String> fileNames, Map<String, int[]> fileToNodeMap,
                                                  Map<Integer, FileOutputStream> nodeIdFileOutputStreamMap, Map<Integer, BufferedOutputStream> nodeIdBufferOutputStreamMap,
                                                  FileOutputStream[][] fileOutputStreams, BufferedOutputStream[][] bufferedOutputStreams, String dataFilePrefix, int numDimensions, int noOfNodes, int maxFiles)
            throws FileNotFoundException {
        boolean usingBufferStream = true;
        long usableMemory = MemoryStatus.getUsableMemory();

        long memoryRequiredForBufferedStream = noOfNodes * 8192 * numDimensions;
        int actualNoOfStreams = 0;
        ;
        if (usableMemory > memoryRequiredForBufferedStream) {
            //if(true){
            for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                int[] nodeIds = fileToNodeMap.get(entry.getValue());
                for (int i = 0; i < numDimensions; i++) {
                    BufferedOutputStream bos = nodeIdBufferOutputStreamMap.get(nodeIds[i]);
                    if (bos == null) {
                        FileOutputStream fos = new FileOutputStream(dataFilePrefix + nodeIds[i]);
                        bos = new BufferedOutputStream(fos);
                        nodeIdFileOutputStreamMap.put(nodeIds[i], fos);
                        nodeIdBufferOutputStreamMap.put(nodeIds[i], bos);
                        actualNoOfStreams++;
                    }
                    bufferedOutputStreams[entry.getKey()][i] = bos;
                }
            }
        } else {
            usingBufferStream = false;
            for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                int[] nodeIds = fileToNodeMap.get(entry.getValue());

                for (int i = 0; i < numDimensions; i++) {
                    FileOutputStream fos = nodeIdFileOutputStreamMap.get(nodeIds[i]);
                    if (fos == null) {
                        fos = new FileOutputStream(dataFilePrefix + nodeIds[i]);
                        nodeIdFileOutputStreamMap.put(nodeIds[i], fos);
                        actualNoOfStreams++;
                    }
                    fileOutputStreams[entry.getKey()][i] = fos;
                }
            }
        }
        logger.info("{} resources allocated for {}", actualNoOfStreams, dataFilePrefix);
        return usingBufferStream;
    }

    public boolean isMemoryAvailableForBuffer(int noOfFiles) {
        long usableMemory = MemoryStatus.getUsableMemory();
        long memoryRequiredForBufferedStream = noOfFiles * 1024;
        return usableMemory > memoryRequiredForBufferedStream;
    }
}
