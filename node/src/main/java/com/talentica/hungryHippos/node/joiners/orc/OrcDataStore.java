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

import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.orc.OrcWriter;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created by rajkishoreh on 7/5/18.
 */
public class OrcDataStore {
    /**
     *
     */
    private static final Logger logger = LoggerFactory.getLogger(OrcDataStoreV3.class);
    private OrcWriter[] orcWriters;
    private String dataFilePrefix;
    private TypeDescription schema;
    private Map<Integer, String> fileNames;
    private String[] columnNames;
    private long tick = 0L;
    private long[] lastUpdated;
    private int fileCount = 0;
    private static final int NUM_OF_FILES_FOR_CLOSE = 25;
    private static final int MAX_OPEN_FILES = 100;
    private int batchSize;


    public OrcDataStore(Map<Integer, String> fileNames, int maxFiles,
                        String hungryHippoFilePath,
                        String uniqueName, TypeDescription schema, String[] columnNames) throws IOException, ClassNotFoundException {
        this.dataFilePrefix = FileSystemContext.getRootDirectory() + hungryHippoFilePath
                + File.separator + uniqueName;
        this.orcWriters = new OrcWriter[maxFiles];
        File file = new File(dataFilePrefix);
        if (!file.exists()) {
            boolean flag = file.mkdirs();
            if (!flag) {
                logger.info("Not able to create dataFolder");
            }
        }
        this.batchSize = Math.max(100,1024/fileNames.size());
        dataFilePrefix = dataFilePrefix + "/";
        this.columnNames = columnNames;
        this.schema = schema;
        this.lastUpdated = new long[maxFiles];
        this.fileNames = fileNames;
        Arrays.fill(lastUpdated,0L);
    }

    public void storeRow(int index, Object[] objects) {
        tick++;
        try {
            if(orcWriters[index]==null){
                orcWriters[index] = new OrcWriter(schema, columnNames, new Path(dataFilePrefix + fileNames.get(index)), batchSize);
                fileCount++;
            }
            orcWriters[index].write(objects);
            this.lastUpdated[index] = tick;
            if(fileCount> MAX_OPEN_FILES){
                closeOldWriters();
            }

        } catch (IOException e) {
            logger.error("Error occurred while writing data received to datastore. {} ", e.toString());
        }
    }

    public void closeOldWriters(){
        List<FileTuple> fileTuples = new ArrayList<>(fileCount);
        for (int i = 0; i < lastUpdated.length; i++) {
            if(lastUpdated[i]>0){
                fileTuples.add(new FileTuple(i,lastUpdated[i]));
            }
        }

        Collections.sort(fileTuples);

        for (int i = 0; i < NUM_OF_FILES_FOR_CLOSE; i++) {
            int index = fileTuples.get(i).index;
            try {
                orcWriters[index].flush();
                orcWriters[index].close();
                orcWriters[index] = null;
                lastUpdated[index] = 0L;
                fileCount--;
            } catch (IOException e) {
                logger.error("Error occurred while flushing " + index + "th output stream. {}", e);
            }
        }
        System.gc();

    }

    static class FileTuple implements Comparable<FileTuple>{
        int index;
        long lastUpdate;

        public FileTuple(int index, long lastUpdate) {
            this.index = index;
            this.lastUpdate = lastUpdate;
        }

        @Override
        public int compareTo(FileTuple other) {
            return Long.compare(lastUpdate,other.lastUpdate);
        }
    }


    public void sync() {
        for (int i = 0; i < orcWriters.length; i++) {
            if (orcWriters[i] != null) {
                try {
                    orcWriters[i].flush();
                    orcWriters[i].close();
                } catch (IOException e) {
                    logger.error("Error occurred while flushing " + i + "th output stream. {}", e);
                }
            }
        }
    }
}
