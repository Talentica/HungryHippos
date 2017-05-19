package com.talentica.hungryHippos.storage;

import org.junit.Test;

import java.util.Iterator;

/**
 * Created by rajkishoreh on 19/5/17.
 */
public class InMemoryDataStoreTest {

    InMemoryDataStore inMemoryDataStore = new InMemoryDataStore("",10,3);

    @Test
    public void testWriteandRead() throws Exception {
        byte[] data = "abcdqwerty".getBytes();
        String fileName = "1";
        for(int i =0; i <1000;i++) {
            String f = i%3+"";
            inMemoryDataStore.storeRow(f, data);
        }
        Iterator<byte[]> iterator = inMemoryDataStore.getIterator(fileName);
        int latestByteArraySize = inMemoryDataStore.getLatestByteArraySize(fileName);
        int byteArrayCount = inMemoryDataStore.getByteArrayCount(fileName);
        int byteArrayBatchSize = inMemoryDataStore.getByteArrayBatchSize();
        while (byteArrayCount > 1) {
            byte[] bytes = iterator.next();
            System.out.println(new String(bytes, 0, byteArrayBatchSize));
            byteArrayCount--;
        }
        if (iterator.hasNext()) {
            byte[] bytes = iterator.next();
            System.out.println(new String(bytes, 0, latestByteArraySize));
        }
    }
}
