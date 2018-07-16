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
