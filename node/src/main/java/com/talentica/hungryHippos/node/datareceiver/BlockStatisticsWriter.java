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

package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryhippos.filesystem.FileStatistics;

import java.io.*;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public class BlockStatisticsWriter implements Callable<Boolean> {

    private String ownedBlockStatisticsFolderPath;
    private Map.Entry<String, FileStatistics> fileStatisticsEntry;

    public BlockStatisticsWriter(String ownedBlockStatisticsFolderPath, Map.Entry<String, FileStatistics> fileStatisticsEntry) {
        this.ownedBlockStatisticsFolderPath = ownedBlockStatisticsFolderPath;
        this.fileStatisticsEntry = fileStatisticsEntry;
    }

    @Override
    public Boolean call() throws Exception {
        writeToFile();
        return true;
    }

    private void writeToFile() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(ownedBlockStatisticsFolderPath+fileStatisticsEntry.getKey(),false);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             ObjectOutput objectOutput = new ObjectOutputStream(bos)
        ) {
            fileStatisticsEntry.getValue().updateFileColumnStatistics();
            objectOutput.writeObject(fileStatisticsEntry.getValue().getBlockStatisticsList());
            objectOutput.flush();
            bos.flush();
            fos.flush();
        }
    }
}
