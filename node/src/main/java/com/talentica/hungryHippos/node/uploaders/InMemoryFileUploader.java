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
package com.talentica.hungryHippos.node.uploaders;

import com.talentica.hungryHippos.node.uploaders.util.CustomFile;
import com.talentica.hungryHippos.storage.InMemoryDataStore;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rajkishoreh on 28/4/17.
 */
public class InMemoryFileUploader extends AbstractFileUploader {

    private Set<String> fileNames;
    private InMemoryDataStore inMemoryDataStore;
    private String srcFolderPath;

    public InMemoryFileUploader(CountDownLatch countDownLatch, String srcFolderPath, String destinationPath,
                                int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Node node,
                                Set<String> fileNames, String hhFilePath, InMemoryDataStore inMemoryDataStore, String tarFilename) {
        super(countDownLatch, srcFolderPath, destinationPath, idx, dataInputStreamMap, socketMap, node, fileNames, hhFilePath,tarFilename);
        this.fileNames = fileNames;
        this.inMemoryDataStore = inMemoryDataStore;
        this.srcFolderPath = srcFolderPath;
    }

    public void createTar(String tarFileName)
            throws IOException {
        File tarFileParentDir = new File(srcFolderPath);
        if (!tarFileParentDir.exists()) {
            tarFileParentDir.mkdirs();
        }
        FileOutputStream out = new FileOutputStream(srcFolderPath + File.separator + tarFileName);
        TarOutputStream tarOut = new TarOutputStream(new BufferedOutputStream(out));
        for (String fileName : fileNames) {
            if (inMemoryDataStore.containsKey(fileName)) {
                readMemoryToTar(tarOut, fileName);
            }
        }
        tarOut.flush();
        out.flush();
        tarOut.close();
        out.close();
    }

    private void readMemoryToTar(TarOutputStream tarOut, String fileName) throws IOException {
        CustomFile customFile = new CustomFile(fileName);
        customFile.initialize(inMemoryDataStore.size(fileName));
        File file = customFile;
        tarOut.putNextEntry(new TarEntry(file, fileName));
        Iterator<byte[]> iterator = inMemoryDataStore.getIterator(fileName);
        int latestByteArraySize = inMemoryDataStore.getLatestByteArraySize(fileName);
        int byteArrayCount = inMemoryDataStore.getByteArrayCount(fileName);
        int byteArrayBatchSize = inMemoryDataStore.getByteArrayBatchSize();
        while (byteArrayCount > 1) {
            byte[] bytes = iterator.next();
            tarOut.write(bytes, 0, byteArrayBatchSize);
            byteArrayCount--;
        }
        if (iterator.hasNext()) {
            byte[] bytes = iterator.next();
            tarOut.write(bytes, 0, latestByteArraySize);
        }
    }

    @Override
    public void writeAppenderType(DataOutputStream dos) throws IOException {
        dos.writeInt(HungryHippoServicesConstants.TAR_DATA_APPENDER);
    }
}
