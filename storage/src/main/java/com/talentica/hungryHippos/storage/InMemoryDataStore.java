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
