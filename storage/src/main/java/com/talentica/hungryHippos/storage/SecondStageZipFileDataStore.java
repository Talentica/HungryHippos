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

package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.FileStatistics;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class SecondStageZipFileDataStore implements DataStore {

    /**
     *
     */
    private static final Logger logger = LoggerFactory.getLogger(FileDataStore.class);
    private Map<String, OutputStream> fileNameToOutputStreamMap;
    private Map<String, FileSystem> fileNameToFileSystemMap;
    private OutputStream[] outputStreams;
    private String hungryHippoFilePath;
    private String dataFilePrefix;
    private boolean usingBufferStream;
    private Map<Integer, String> fileNames;
    private FileStatistics[] fileStatistics;

    public SecondStageZipFileDataStore(Map<Integer, String> fileNames, int maxFiles,
                                       String hungryHippoFilePath,
                                       String fileName, FileStatistics[] fileStatistics) throws IOException, ClassNotFoundException {

        fileNameToOutputStreamMap = new HashMap<>();
        fileNameToFileSystemMap = new HashMap<>();
        this.fileStatistics = fileStatistics;
        this.hungryHippoFilePath = hungryHippoFilePath;
        this.dataFilePrefix = FileSystemContext.getRootDirectory() + hungryHippoFilePath
                + File.separator + fileName;
        this.outputStreams = new OutputStream[maxFiles];
        File file = new File(dataFilePrefix);
        if (!file.exists()) {
            boolean flag = file.mkdirs();
            if (!flag) {
                logger.info("Not able to create dataFolder");
            }
        }
        dataFilePrefix = dataFilePrefix + "/";
        this.fileNames = fileNames;
        usingBufferStream = ResourceAllocator.INSTANCE.allocateZipResources(fileNames, this.outputStreams,
                this.dataFilePrefix, this.fileNameToOutputStreamMap, this.fileNameToFileSystemMap,
                false, null, fileStatistics);
    }

    @Override
    public boolean storeRow(String name, byte[] raw) {
        try {
            fileNameToOutputStreamMap.get(name).write(raw);
            return true;
        } catch (NullPointerException e) {
            logger.error(name + " not present");
            return false;
        } catch (IOException e) {
            logger.error("Error occurred while writing data received to datastore. {} ", e.toString());
            return false;
        }
    }

    @Override
    public void storeRow(int index, byte[] raw) {
        try {
            outputStreams[index].write(raw);
        } catch (IOException e) {
            logger.error("Error occurred while writing data received to datastore. {} ", e.toString());
        }
    }

    public void storeRow(int index, byte[] raw, int off, int len) {
        try {
            outputStreams[index].write(raw, off, len);
        } catch (IOException e) {
            logger.error("Error occurred while writing data received to datastore. {} ", e.toString());
        }
    }

    @Override
    public void sync() {

        for (int i = 0; i < outputStreams.length; i++) {
            if (outputStreams[i] != null) {
                try {
                    String fileName = fileNames.get(i);
                    try {
                        outputStreams[i].flush();
                    } catch (IOException e) {
                        logger.error("Error occurred while flushing " + i + "th output stream. {}", e);
                    } finally {
                        if (outputStreams[i] != null) {
                            outputStreams[i].close();
                        }
                    }

                    FileSystem zipFileSystem = fileNameToFileSystemMap.get(fileName);
                    try (OutputStream os = Files.newOutputStream(zipFileSystem.getPath(FileSystemConstants.ZIP_METADATA_FILENAME), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                         ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
                         ObjectOutput objectOutput = new ObjectOutputStream(arrayOutputStream);
                    ) {
                        fileStatistics[i].updateFileColumnStatistics();
                        objectOutput.writeObject(fileStatistics[i].getBlockStatisticsList());
                        objectOutput.flush();
                        os.write(arrayOutputStream.toByteArray());
                        os.flush();
                    } finally {
                        if (zipFileSystem != null) {
                            zipFileSystem.close();
                        }
                    }

                } catch (IOException e) {
                    logger.error("Error occurred while processing " + i + "th output stream. {}", e);
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

    public boolean isUsingBufferStream() {
        return usingBufferStream;
    }

    public void upgradeStreams() throws IOException, ClassNotFoundException {
        usingBufferStream = ResourceAllocator.INSTANCE.allocateZipResources(fileNames, this.outputStreams,
                this.dataFilePrefix, this.fileNameToOutputStreamMap, this.fileNameToFileSystemMap, true, this, fileStatistics);
        if (usingBufferStream) {
            logger.info("Upgraded to BufferedStreams for {}", dataFilePrefix);
        }
    }

    public Map<String, OutputStream> getFileNameToOutputStreamMap() {
        return fileNameToOutputStreamMap;
    }

}
