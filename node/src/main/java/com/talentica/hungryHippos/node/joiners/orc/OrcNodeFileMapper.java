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
package com.talentica.hungryHippos.node.joiners.orc;

import com.talentica.hungryHippos.node.datareceiver.ApplicationCache;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by rajkishoreh on 18/5/17.
 */
class OrcNodeFileMapper {

    private OrcDataStore orcDataStore;
    private String uniqueFolderName;

    public OrcNodeFileMapper(String hhFilePath, Map<Integer, String> fileNames) {
        this.uniqueFolderName = UUID.randomUUID().toString();
        this.orcDataStore = new OrcDataStore(fileNames, ApplicationCache.INSTANCE.getMaxFiles(hhFilePath),
                hhFilePath, uniqueFolderName,ApplicationCache.INSTANCE.getSchema(hhFilePath),ApplicationCache.INSTANCE.getColumnNames(hhFilePath));
    }

    public void storeRow(int index, Object[] objects) {
        orcDataStore.storeRow(index, objects);
    }

    public void sync() {
        orcDataStore.sync();
    }


    public String getUniqueFolderName() {
        return uniqueFolderName;
    }

}
