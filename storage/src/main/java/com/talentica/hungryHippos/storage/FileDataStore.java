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
package com.talentica.hungryHippos.storage;

import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileDataStore implements DataStore {
    /**
     *
     */
    private static final Logger logger = LoggerFactory.getLogger(FileDataStore.class);
    private Map<String, FileOutputStream> fileNameToOutputStreamMap;
    private Map<String, BufferedOutputStream> fileNameToBufferedOutputStreamMap;
    private OutputStream[] outputStreams;
    private String hungryHippoFilePath;
    private String dataFilePrefix;
    private boolean usingBufferStream;
    private Map<Integer, String> fileNames;

    public FileDataStore(Map<Integer, String> fileNames, int maxBucketSize, int numDimensions,
                         String hungryHippoFilePath, String fileName) throws IOException, InterruptedException, ClassNotFoundException,
            KeeperException, JAXBException {
        this(fileNames, maxBucketSize, numDimensions, hungryHippoFilePath, false, fileName);
    }

    public FileDataStore(Map<Integer, String> fileNames, int maxBucketSize, int numDimensions,
                         String hungryHippoFilePath, boolean append,
                         String fileName) throws IOException {

        fileNameToOutputStreamMap = new HashMap<>();
        fileNameToBufferedOutputStreamMap = new HashMap<>();
        this.hungryHippoFilePath = hungryHippoFilePath;
        this.dataFilePrefix = FileSystemContext.getRootDirectory() + hungryHippoFilePath
                + File.separator + fileName;

        int maxFiles = (int) Math.pow(maxBucketSize, numDimensions);
        this.outputStreams = new OutputStream[maxFiles];
        File file = new File(dataFilePrefix);
        if (!file.exists()) {
            boolean flag = file.mkdirs();
            if (flag) {
                logger.info("created data folder");
            } else {
                logger.info("Not able to create dataFolder");
            }
        }
        dataFilePrefix = dataFilePrefix + "/";
        this.fileNames = fileNames;
        usingBufferStream = ResourceAllocator.INSTANCE.allocateResources(fileNames, this.outputStreams,
                this.dataFilePrefix, this.fileNameToOutputStreamMap,this.fileNameToBufferedOutputStreamMap, append, false, null);
        if(usingBufferStream) {
            logger.info("Using BufferedStreams for {}", dataFilePrefix);
        }
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
                    outputStreams[i].flush();
                } catch (IOException e) {
                    logger.error("Error occurred while flushing " + i + "th output stream. {}", e);
                }
            }
        }
        for (Map.Entry<String, FileOutputStream> nameStreamEntry : fileNameToOutputStreamMap.entrySet()) {
            try {
                nameStreamEntry.getValue().flush();
            } catch (IOException e) {
                logger.error("Error occurred while flushing " + nameStreamEntry.getKey() + " output stream. {}", e);
            } finally {
                try {
                    if (nameStreamEntry.getValue() != null)
                        nameStreamEntry.getValue().close();
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

    public boolean isUsingBufferStream() {
        return usingBufferStream;
    }

    public void upgradeStreams() throws FileNotFoundException {
        usingBufferStream = ResourceAllocator.INSTANCE.allocateResources(fileNames, this.outputStreams,
                this.dataFilePrefix, this.fileNameToOutputStreamMap, this.fileNameToBufferedOutputStreamMap,true, true, this);
        if(usingBufferStream){
            logger.info("Upgraded to BufferedStreams for {}",dataFilePrefix);
        }
    }

    public Map<String, FileOutputStream> getFileNameToOutputStreamMap() {
        return fileNameToOutputStreamMap;
    }

    public Map<String, BufferedOutputStream> getFileNameToBufferedOutputStreamMap() {
        return fileNameToBufferedOutputStreamMap;
    }
}
