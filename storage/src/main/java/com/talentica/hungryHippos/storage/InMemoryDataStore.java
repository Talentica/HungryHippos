package com.talentica.hungryHippos.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by rajkishoreh on 27/4/17.F
 */
public class InMemoryDataStore extends AbstractInMemoryDataStore {

    public InMemoryDataStore(String hhFilePath, int byteArraySize, int noOfFiles) {
        super(hhFilePath,byteArraySize,noOfFiles);
    }

    @Override
    public boolean handleOnMaxKeysCount(String key, byte[] arr) throws IOException {
        return false;
    }

}
