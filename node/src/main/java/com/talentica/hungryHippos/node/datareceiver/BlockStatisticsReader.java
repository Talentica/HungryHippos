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

import com.talentica.hungryhippos.filesystem.BlockStatistics;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public class BlockStatisticsReader implements Callable<List<BlockStatistics>> {
    private String blockStatisticsFilePath;

    public BlockStatisticsReader(String blockStatisticsFilePath) {
        this.blockStatisticsFilePath = blockStatisticsFilePath;
    }

    @Override
    public List<BlockStatistics> call() throws Exception {
        return readBlockStatistics();
    }

    private List<BlockStatistics> readBlockStatistics() throws IOException, ClassNotFoundException {
        if (new File(blockStatisticsFilePath).exists()) {
            List<BlockStatistics> blockStatisticsList = null;
            try (FileInputStream fis = new FileInputStream(blockStatisticsFilePath);
                 BufferedInputStream bis = new BufferedInputStream(fis);
                 ObjectInputStream ois = new ObjectInputStream(bis)) {
                blockStatisticsList = (List<BlockStatistics>) ois.readObject();
            }
            return blockStatisticsList;
        }
        return new LinkedList<>();

    }
}
