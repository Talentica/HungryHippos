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
package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.datareceiver.HHFileNamesIdentifier;
import com.talentica.hungryHippos.node.datareceiver.ShardingResourceCache;
import com.talentica.hungryHippos.node.datareceiver.SynchronousFolderDeleter;
import com.talentica.hungryHippos.node.uploaders.HHFileUploader;
import com.talentica.hungryHippos.sharding.util.NodeSelector;
import com.talentica.hungryHippos.storage.*;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by rajkishoreh on 6/2/17.
 */
public class HHFileMapper {

    private Map<Integer, Set<String>> nodeToFileMap;
    private Map<String, int[]> fileToNodeMap;
    private DataStore dataStore;
    private String hhFilePath;
    private String uniqueFolderName;

    public HHFileMapper(String hhFilePath, int byteArraySize, int numDimensions, StoreType storeType) throws InterruptedException, ClassNotFoundException, JAXBException, KeeperException, IOException {
        this.hhFilePath = hhFilePath;
        HHFileNamesIdentifier hhFileNamesIdentifier = ShardingResourceCache.INSTANCE.getHHFileNamesCalculator(hhFilePath);
        Map<Integer, String> fileNames = hhFileNamesIdentifier.getFileNames();
        nodeToFileMap = hhFileNamesIdentifier.getNodeToFileMap();
        fileToNodeMap = hhFileNamesIdentifier.getFileToNodeMap();
        this.uniqueFolderName = UUID.randomUUID().toString();
        switch (storeType){
            case FILEDATASTORE:
                dataStore = new FileDataStore(fileNames, ShardingResourceCache.INSTANCE.getMaxFiles(hhFilePath),
                        hhFilePath, uniqueFolderName);
                break;
            case INMEMORYDATASTORE:
                dataStore = new InMemoryDataStore(hhFilePath,byteArraySize,fileNames.size());
                break;
            case HYBRIDDATASTORE:
                dataStore = new HybridDataStore(hhFilePath,byteArraySize,fileNames.size(),uniqueFolderName);
                break;
            case NODEWISEDATASTORE:
                NodeSelector nodeSelector = new NodeSelector();
                dataStore =  new NodeWiseDataStore(fileNames,fileToNodeMap, ShardingResourceCache.INSTANCE.getMaxFiles(hhFilePath), numDimensions,
                        hhFilePath, uniqueFolderName,nodeSelector.noOfNodes());
                break;
        }
    }

    public void storeRow(int index, byte[] raw) {
        dataStore.storeRow(index,raw);
    }

    public boolean storeRow(String key, byte[] raw) throws IOException {
        return dataStore.storeRow(key,raw);
    }

    public void sync() throws IOException, InterruptedException {
        dataStore.sync();
        String baseFolderPath = FileSystemContext.getRootDirectory() + hhFilePath;
        String srcFolderPath = baseFolderPath + File.separator + uniqueFolderName;
        String destFolderPath = baseFolderPath + File.separator + FileSystemContext.getDataFilePrefix();
        HHFileUploader.INSTANCE.uploadFile(srcFolderPath, destFolderPath, nodeToFileMap, hhFilePath,dataStore);
        dataStore.reset();
        SynchronousFolderDeleter.INSTANCE.deleteEmptyFolder(new File(srcFolderPath));
    }

}
