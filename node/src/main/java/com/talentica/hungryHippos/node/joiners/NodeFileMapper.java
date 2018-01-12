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
package com.talentica.hungryHippos.node.joiners;

import com.talentica.hungryHippos.node.datareceiver.ApplicationCache;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by rajkishoreh on 18/5/17.
 */
class NodeFileMapper {

    private FileDataStore fileDataStore;
    private String uniqueFolderName;
    private Map<Integer, String> fileNames;

    public NodeFileMapper(String hhFilePath) throws InterruptedException, ClassNotFoundException,
            JAXBException, KeeperException, IOException {
        fileNames = ApplicationCache.INSTANCE.getIndexToFileNamesForFirstDimension(hhFilePath);
        this.uniqueFolderName = FileSystemContext.getDataFilePrefix();
        this.fileDataStore = new FileDataStore(fileNames, ApplicationCache.INSTANCE.getMaxFiles(hhFilePath),
                hhFilePath, true, uniqueFolderName);
    }

    public void storeRow(int index, byte[] raw, int off, int len) {
        fileDataStore.storeRow(index, raw, off, len);
    }

    public void sync() throws IOException, InterruptedException {
        fileDataStore.sync();
    }

    public boolean isUsingBufferStream() {
        return fileDataStore.isUsingBufferStream();
    }

    public int getNoOfFiles() {
        return fileNames.size();
    }

    public void upgradeStore() throws IOException {
        this.fileDataStore.upgradeStreams();
    }

    public Map<String, FileOutputStream> getFileNameToOutputStreamMap() {
        return this.fileDataStore.getFileNameToOutputStreamMap();
    }

    public Map<String, BufferedOutputStream> getFileNameToBufferedOutputStreamMap() {
        return this.fileDataStore.getFileNameToBufferedOutputStreamMap();
    }
}
