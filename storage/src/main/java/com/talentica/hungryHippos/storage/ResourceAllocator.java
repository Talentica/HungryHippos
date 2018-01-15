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

import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryhippos.filesystem.BlockStatistics;
import com.talentica.hungryhippos.filesystem.FileStatistics;

import java.io.*;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by rajkishoreh on 18/5/17.
 */
public enum ResourceAllocator {
    INSTANCE;

    private Object lockObj = new Object();


    public boolean allocateResources(Map<Integer, String> fileNames, OutputStream[] outputStreams,
                                                  String dataFilePrefix, Map<String, FileOutputStream> fileNameToOutputStreamMap,Map<String, BufferedOutputStream> fileNameToBufferedOutputStreamMap,
                                                  boolean append, boolean reqForUpgrade, FileDataStore fileDataStore) throws FileNotFoundException {

        boolean usingBufferStream = true;
        System.gc();
        long memoryRequiredForBufferedStream = fileNames.size() * 8192;
        synchronized (lockObj) {
            long usableMemory = MemoryStatus.getUsableMemory();

            if (usableMemory > memoryRequiredForBufferedStream) {
                if(reqForUpgrade) {
                    fileDataStore.sync();
                }
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    FileOutputStream fos = new FileOutputStream(dataFilePrefix + entry.getValue(), append);
                    BufferedOutputStream bos = new BufferedOutputStream(fos);
                    outputStreams[entry.getKey()] = bos;
                    fileNameToOutputStreamMap.put(entry.getValue(), fos);
                    fileNameToBufferedOutputStreamMap.put(entry.getValue(), bos);
                }
            } else {
                usingBufferStream = false;
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    FileOutputStream fos = new FileOutputStream(dataFilePrefix + entry.getValue(), append);
                    outputStreams[entry.getKey()] = fos;
                    fileNameToOutputStreamMap.put(entry.getValue(), fos);
                }
            }
        }
        return usingBufferStream;
    }

    public boolean allocateResources(Map<Integer, String> fileNames, OutputStream[] outputStreams,
                                     String dataFilePrefix, Map<String, FileOutputStream> fileNameToOutputStreamMap, Map<String, BufferedOutputStream> fileNameToBufferedOutputStreamMap,
                                     boolean append, boolean reqForUpgrade, FirstDimensionFileDataStore fileDataStore, FileStatistics[] fileStatistics) throws IOException, ClassNotFoundException {

        boolean usingBufferStream = true;
        System.gc();

        long memoryRequiredForBufferedStream = fileNames.size() * 16384;
        synchronized (lockObj) {
            long usableMemory = MemoryStatus.getUsableMemory();

            if (usableMemory > memoryRequiredForBufferedStream) {
                if(reqForUpgrade) {
                    fileDataStore.sync();
                }
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {

                    if(!reqForUpgrade){
                        String zipPath = dataFilePrefix + entry.getValue() + FileSystemConstants.ZIP_EXTENSION;
                        if(new File(zipPath).exists()) {
                            try(FileSystem zipFS = ZipFileSystemHandler.INSTANCE.get(zipPath);){
                                readZipBlockStats(fileStatistics[entry.getKey()], zipFS);
                            }

                        }
                    }
                    FileOutputStream fos = new FileOutputStream(dataFilePrefix + entry.getValue(), append);
                    BufferedOutputStream bos = new BufferedOutputStream(fos);
                    outputStreams[entry.getKey()] = bos;
                    fileNameToOutputStreamMap.put(entry.getValue(), fos);
                    fileNameToBufferedOutputStreamMap.put(entry.getValue(), bos);
                }
            } else {
                usingBufferStream = false;
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    if(!reqForUpgrade){
                        String zipPath = dataFilePrefix + entry.getValue() + FileSystemConstants.ZIP_EXTENSION;
                        if(new File(zipPath).exists()) {
                            try(FileSystem zipFS = ZipFileSystemHandler.INSTANCE.get(zipPath);){
                                readZipBlockStats(fileStatistics[entry.getKey()], zipFS);
                            }

                        }
                    }
                    FileOutputStream fos = new FileOutputStream(dataFilePrefix + entry.getValue(), append);
                    outputStreams[entry.getKey()] = fos;
                    fileNameToOutputStreamMap.put(entry.getValue(), fos);
                }
            }
        }
        return usingBufferStream;
    }

    public boolean allocateResources(Map<Integer, String> fileNames, OutputStream[] outputStreams,
                                                  String dataFilePrefix, Map<String, FileOutputStream> fileNameToOutputStreamMap, Map<String, BufferedOutputStream> fileNameToBufferedOutputStreamMap,
                                                  boolean append, boolean reqForUpgrade, FileDataStoreFirstStage fileDataStore, int reducefactor) throws IOException {

        boolean usingBufferStream = true;
        System.gc();
        long memoryRequiredForBufferedStream = fileNames.size() > reducefactor ? reducefactor*8192 :fileNames.size() * 8192;
        synchronized (lockObj) {
            long usableMemory = MemoryStatus.getUsableMemory();
            if (usableMemory > memoryRequiredForBufferedStream) {
                if(reqForUpgrade) {
                    fileDataStore.sync();
                }
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    String interMediateFileId = entry.getValue();
                    BufferedOutputStream bos = fileNameToBufferedOutputStreamMap.get(interMediateFileId);
                    if (bos == null) {
                        FileOutputStream fos = new FileOutputStream(dataFilePrefix + interMediateFileId, append);
                        bos = new BufferedOutputStream(fos);
                        fileNameToOutputStreamMap.put(interMediateFileId, fos);
                        fileNameToBufferedOutputStreamMap.put(interMediateFileId, bos);
                    }
                    outputStreams[entry.getKey()] = bos;
                }
            } else {
                usingBufferStream = false;
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    String interMediateFileId = entry.getValue();
                    FileOutputStream fos = fileNameToOutputStreamMap.get(interMediateFileId);
                    if (fos == null) {
                        fos = new FileOutputStream(dataFilePrefix + interMediateFileId, append);
                        fileNameToOutputStreamMap.put(interMediateFileId, fos);
                    }
                    outputStreams[entry.getKey()] = fos;
                }
            }
        }
        return usingBufferStream;
    }

    public boolean allocateZipResources(Map<Integer, String> fileNames, OutputStream[] outputStreams,
                                        String dataFilePrefix, Map<String, OutputStream> fileNameToOutputStreamMap, Map<String, FileSystem> zipFileSystemMap,
                                        boolean reqForUpgrade, SecondStageZipFileDataStore secondStageZipFileDataStore, FileStatistics[] fileStatistics) throws IOException, ClassNotFoundException {
        Map<String, Object> zipFileSystemEnv = new HashMap<>();
        zipFileSystemEnv.put("create", "true");
        zipFileSystemEnv.put("useTempFile", Boolean.TRUE);
        boolean usingBufferStream = true;
        System.gc();
        String uriDataFilePrefix = "jar:file:" + dataFilePrefix;

        long memoryRequiredForBufferedStream = fileNames.size() * 10240;
        synchronized (lockObj) {
            long usableMemory = MemoryStatus.getUsableMemory();

            if (usableMemory > memoryRequiredForBufferedStream) {
                if (reqForUpgrade) {
                    secondStageZipFileDataStore.sync();
                }
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    URI uri = URI.create(uriDataFilePrefix + entry.getValue() + FileSystemConstants.ZIP_EXTENSION);
                    FileSystem zipFS = null;
                    try {
                        zipFS = FileSystems.newFileSystem(uri, zipFileSystemEnv);
                    } catch (FileSystemAlreadyExistsException e) {
                        zipFS = FileSystems.getFileSystem(uri);
                    }
                    if(!reqForUpgrade){
                        readZipBlockStats(fileStatistics[entry.getKey()], zipFS);
                    }
                    OutputStream os = Files.newOutputStream(zipFS.getPath(FileSystemConstants.ZIP_DATA_FILENAME), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    outputStreams[entry.getKey()] = os;
                    fileNameToOutputStreamMap.put(entry.getValue(), os);
                    zipFileSystemMap.put(entry.getValue(), zipFS);
                }

            } else {
                zipFileSystemEnv.put("useTempFile", Boolean.TRUE);
                usingBufferStream = false;
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    URI uri = URI.create(uriDataFilePrefix + entry.getValue() + FileSystemConstants.ZIP_EXTENSION);
                    FileSystem zipFS = null;
                    try {
                        zipFS = FileSystems.newFileSystem(uri, zipFileSystemEnv);
                    } catch (FileSystemAlreadyExistsException e) {
                        zipFS = FileSystems.getFileSystem(uri);
                    }
                    if(!reqForUpgrade){
                        readZipBlockStats(fileStatistics[entry.getKey()], zipFS);
                    }
                    OutputStream os = Files.newOutputStream(zipFS.getPath(FileSystemConstants.ZIP_DATA_FILENAME), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    outputStreams[entry.getKey()] = os;
                    fileNameToOutputStreamMap.put(entry.getValue(), os);
                    zipFileSystemMap.put(entry.getValue(), zipFS);
                }
            }
        }
        return usingBufferStream;
    }

    private void readZipBlockStats(FileStatistics fileStatistic, FileSystem zipFS) throws IOException, ClassNotFoundException {
        long newPos = fileStatistic.getDataSize();
        if (newPos > 0) {
            try (InputStream is = Files.newInputStream(zipFS.getPath(FileSystemConstants.ZIP_METADATA_FILENAME));
                 ObjectInputStream ois = new ObjectInputStream(is)) {
                fileStatistic.setBlockStatisticsList((List<BlockStatistics>) ois.readObject());
            }
        } else {
            fileStatistic.setBlockStatisticsList(new LinkedList<>());
        }
    }

    private void readBlockStatistics(boolean reqForUpgrade, FileStatistics[] fileStatistics, Map.Entry<Integer, String> entry, File file, long newPos) throws IOException, ClassNotFoundException {
        if(!reqForUpgrade) {
            if (newPos > 0) {
                try (FileInputStream fis = new FileInputStream(file);) {
                    fis.skip(newPos);
                    try (ObjectInputStream ois = new ObjectInputStream(fis)) {
                        fileStatistics[entry.getKey()].setBlockStatisticsList((List<BlockStatistics>) ois.readObject());
                    }
                }
            } else {
                fileStatistics[entry.getKey()].setBlockStatisticsList(new LinkedList<>());
            }
        }
    }



    public boolean allocateResources(Map<Integer, String> fileNames, Map<String, int[]> fileToNodeMap,
                                                  Map<Integer, FileOutputStream> nodeIdFileOutputStreamMap, Map<Integer, BufferedOutputStream> nodeIdBufferOutputStreamMap,
                                                  FileOutputStream[][] fileOutputStreams, BufferedOutputStream[][] bufferedOutputStreams, String dataFilePrefix, int numDimensions, int noOfNodes, int maxFiles)
            throws FileNotFoundException {
        boolean usingBufferStream = true;
        System.gc();
        long memoryRequiredForBufferedStream = noOfNodes * 8192 * numDimensions;
        synchronized (lockObj) {
            long usableMemory = MemoryStatus.getUsableMemory();

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
                        }
                        fileOutputStreams[entry.getKey()][i] = fos;
                    }
                }
            }
        }
        return usingBufferStream;
    }

    public boolean allocateResourcesForFirstDimension(Map<Integer, String> fileNames, Map<String, Integer> fileToNodeMap,
                                                      Map<Integer, FileOutputStream> nodeIdFileOutputStreamMap, Map<Integer, BufferedOutputStream> nodeIdBufferOutputStreamMap,
                                                      FileOutputStream[] fileOutputStreams, BufferedOutputStream[] bufferedOutputStreams, String dataFilePrefix, int noOfNodes)
            throws FileNotFoundException {
        boolean usingBufferStream = true;
        System.gc();
        long memoryRequiredForBufferedStream = noOfNodes * 10240;
        synchronized (lockObj) {
            long usableMemory = MemoryStatus.getUsableMemory();

            if (usableMemory > memoryRequiredForBufferedStream) {
                //if(true){
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    int nodeId = fileToNodeMap.get(entry.getValue());
                    BufferedOutputStream bos = nodeIdBufferOutputStreamMap.get(nodeId);
                    if (bos == null) {
                        FileOutputStream fos = new FileOutputStream(dataFilePrefix + nodeId);
                        bos = new BufferedOutputStream(fos);
                        nodeIdFileOutputStreamMap.put(nodeId, fos);
                        nodeIdBufferOutputStreamMap.put(nodeId, bos);
                    }
                    bufferedOutputStreams[entry.getKey()] = bos;

                }
            } else {
                usingBufferStream = false;
                for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                    int nodeId = fileToNodeMap.get(entry.getValue());

                    FileOutputStream fos = nodeIdFileOutputStreamMap.get(nodeId);
                    if (fos == null) {
                        fos = new FileOutputStream(dataFilePrefix + nodeId);
                        nodeIdFileOutputStreamMap.put(nodeId, fos);
                    }
                    fileOutputStreams[entry.getKey()] = fos;
                }
            }
        }
        return usingBufferStream;
    }

    public boolean isMemoryAvailableForBuffer(int noOfFiles) {
        long usableMemory = MemoryStatus.getUsableMemory();
        long memoryRequiredForBufferedStream = noOfFiles * 10240;
        return usableMemory > memoryRequiredForBufferedStream;
    }
}
