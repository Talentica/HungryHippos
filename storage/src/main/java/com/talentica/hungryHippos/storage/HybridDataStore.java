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


import java.io.*;
import java.util.*;

/**
 * Created by rajkishoreh on 12/5/17.
 */
public class HybridDataStore extends AbstractInMemoryDataStore {

    private String dataFolderPath;
    private Set<String> dataInFiles;

    public HybridDataStore(String hhFilePath, int byteArraySize, int noOfFiles,String folderName) {
        super(hhFilePath,byteArraySize,noOfFiles);
        if(noOfFiles>super.getMaxKeys()){
            this.dataInFiles = new HashSet<>();
            this.dataFolderPath = FileSystemContext.getRootDirectory() + hhFilePath
                    + File.separator + folderName+File.separator;
            new File(dataFolderPath).mkdirs();
        }
    }


    @Override
    public boolean handleOnMaxKeysCount(String key, byte[] arr) throws IOException {
        dataInFiles.add(key);
        FileOutputStream fos = new FileOutputStream(dataFolderPath+key,true);
        fos.write(arr);
        fos.flush();
        fos.close();
        return true;
    }

    public boolean containsKeyInFile(String key){
        return dataInFiles.contains(key);
    }

    public String getDataFolderPath() {
        return dataFolderPath;
    }
}
