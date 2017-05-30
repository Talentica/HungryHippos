package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.storage.util.Counter;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
