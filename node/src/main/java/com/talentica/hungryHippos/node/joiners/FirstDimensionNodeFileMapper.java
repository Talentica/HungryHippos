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

import com.talentica.hungryHippos.node.datareceiver.ApplicationCache;
import com.talentica.hungryHippos.storage.FirstDimensionFileDataStore;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class FirstDimensionNodeFileMapper {

    private FirstDimensionFileDataStore fileDataStore;
    private String uniqueFolderName;
    private Map<Integer, String> fileNames;

    public FirstDimensionNodeFileMapper(String hhFilePath, Map<Integer, String> fileNames) throws InterruptedException, ClassNotFoundException,
            JAXBException, KeeperException, IOException {
        this.fileNames = fileNames;
        this.uniqueFolderName = UUID.randomUUID().toString();
        this.fileDataStore = new FirstDimensionFileDataStore(fileNames, ApplicationCache.INSTANCE.getMaxFiles(hhFilePath),
                hhFilePath, false, uniqueFolderName, ApplicationCache.INSTANCE.getFileStatisticsMap(hhFilePath));
    }

    public void storeRow(int index, byte[] raw, int off, int len) {
        fileDataStore.storeRow(index, raw, off, len);
    }

    public void sync() throws IOException, InterruptedException {
        fileDataStore.sync();
        fileDataStore=null;
        System.gc();
    }

    public boolean isUsingBufferStream() {
        return fileDataStore.isUsingBufferStream();
    }

    public int getNoOfFiles() {
        return fileNames.size();
    }

    public void upgradeStore() throws IOException, ClassNotFoundException {
        this.fileDataStore.upgradeStreams();
    }

    public String getUniqueFolderName() {
        return uniqueFolderName;
    }
}
