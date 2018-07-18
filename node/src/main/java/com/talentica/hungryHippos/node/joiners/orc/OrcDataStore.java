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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by rajkishoreh on 7/5/18.
 */
public class OrcDataStore {
    /**
     *
     */
    private static final Logger logger = LoggerFactory.getLogger(OrcDataStore.class);
    private OrcWriter[] orcWriters;
    private String dataFilePrefix;
    private TypeDescription schema;
    private Map<Integer, String> fileNames;
    private String[] columnNames;
    private long tick = 0L;
    private long[] lastUpdated;
    private int fileCount = 0;
    private static final int NUM_OF_FILES_FOR_CLOSE = 64;
    private static final int MAX_OPEN_FILES = 512;
    private int batchSize;
    private static final String CURR_FILE_EXTENSION = ".CURR";
    private static final String MAIN_FILE_EXTENSION = ".MAIN";
    private boolean[] hasMainFile;
    private ExecutorService service;
    private List<OrcDataMerger> orcDataMergers;
    private List<OrcDataReWriter> orcDataReWriters;
    private final static int NO_OF_DATA_MERGER = 2 * Math.max(Runtime.getRuntime().availableProcessors(),1);
    private List<Future<Boolean>> orcDataMergerFutures;


    public OrcDataStore(Map<Integer, String> fileNames, int maxFiles,
                        String hungryHippoFilePath,
                        String uniqueName, TypeDescription schema, String[] columnNames) {
        this.dataFilePrefix = FileSystemContext.getRootDirectory() + hungryHippoFilePath
                + File.separator + uniqueName;
        this.orcWriters = new OrcWriter[maxFiles];
        this.hasMainFile = new boolean[maxFiles];
        Arrays.fill(hasMainFile, false);
        File file = new File(dataFilePrefix);
        if (!file.exists()) {
            boolean flag = file.mkdirs();
            if (!flag) {
                logger.info("Not able to create dataFolder");
            }
        }
        this.batchSize = Math.max(64, 1024 / fileNames.size());
        dataFilePrefix = dataFilePrefix + "/";
        this.columnNames = columnNames;
        this.schema = schema;
        this.lastUpdated = new long[maxFiles];
        this.fileNames = fileNames;
        Arrays.fill(lastUpdated, 0L);
        this.orcDataMergers = new ArrayList<>();
        this.orcDataMergerFutures = new ArrayList<>();
        this.service = Executors.newFixedThreadPool(NO_OF_DATA_MERGER);
        for (int i = 0; i < NO_OF_DATA_MERGER; i++) {
            OrcDataMerger orcDataMerger = new OrcDataMerger(new ConcurrentLinkedQueue<>());
            orcDataMergerFutures.add(service.submit(orcDataMerger));
            orcDataMergers.add(orcDataMerger);
        }
    }

    public void storeRow(int index, Object[] objects) {
        tick++;
        try {
            if (orcWriters[index] == null) {
                orcWriters[index] = new OrcWriter(schema, columnNames, new Path(dataFilePrefix + fileNames.get(index) + CURR_FILE_EXTENSION), batchSize);
                fileCount++;
            }
            orcWriters[index].write(objects);
            this.lastUpdated[index] = tick;
            if (fileCount > MAX_OPEN_FILES) {
                closeOldWriters();
            }

        } catch (IOException e) {
            logger.error("Error occurred while writing data received to datastore. {} ", e.toString());
        }
    }

    public void closeOldWriters() {
        List<FileTuple> fileTuples = new ArrayList<>(fileCount);
        for (int i = 0; i < lastUpdated.length; i++) {
            if (lastUpdated[i] > 0) {
                fileTuples.add(new FileTuple(i, lastUpdated[i]));
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
                mergeToMain(index);
                fileCount--;
            } catch (IOException e) {
                logger.error("Error occurred while flushing " + index + "th output stream. {}", e);
            }
        }

    }

    private void mergeToMain(int index) {
        String finalPathString = dataFilePrefix + fileNames.get(index);
        String currPathString = finalPathString + CURR_FILE_EXTENSION;
        this.hasMainFile[index] = true;
        int mergerIndex = getMergerIndex(index, finalPathString);
        String renamePathString = finalPathString + UUID.randomUUID().toString();
        new File(currPathString).renameTo(new File(renamePathString));
        this.orcDataMergers.get(mergerIndex).addDataEntities(new OrcStoreIncrementalDataEntity(new String[]{renamePathString}, finalPathString + MAIN_FILE_EXTENSION));
    }

    private int getMergerIndex(int index, String finalPathString) {
        return ((finalPathString.hashCode() + index) % NO_OF_DATA_MERGER + NO_OF_DATA_MERGER) % NO_OF_DATA_MERGER;
    }

    static class FileTuple implements Comparable<FileTuple> {
        int index;
        long lastUpdate;

        public FileTuple(int index, long lastUpdate) {
            this.index = index;
            this.lastUpdate = lastUpdate;
        }

        @Override
        public int compareTo(FileTuple other) {
            return Long.compare(lastUpdate, other.lastUpdate);
        }
    }


    public void sync() {

        for (OrcDataMerger orcDataMerger : this.orcDataMergers) {
            orcDataMerger.kill();
        }
        for (Future<Boolean> future : orcDataMergerFutures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                logger.error("Error occurred while merging {}", e);
            }
        }

        orcDataReWriters = new ArrayList<>();

        List<Future<Boolean>> orcReWriteFutures = new ArrayList<>();
        for (int i = 0; i < NO_OF_DATA_MERGER; i++) {
            OrcDataReWriter orcDataReWriter = new OrcDataReWriter(new ConcurrentLinkedQueue<>());
            orcReWriteFutures.add(service.submit(orcDataReWriter));
            orcDataReWriters.add(orcDataReWriter);
        }


        for (int index = 0; index < orcWriters.length; index++) {
            if (orcWriters[index] != null) {
                try {
                    orcWriters[index].flush();
                    orcWriters[index].close();
                    orcWriters[index] = null;
                    mergeToFinal(index);
                } catch (IOException e) {
                    logger.error("Error occurred while flushing " + index + "th output stream. {}", e);
                }
            } else if (this.hasMainFile[index]) {
                String finalPathString = dataFilePrefix + fileNames.get(index);
                int mergeIndex = getMergerIndex(index, finalPathString);
                orcDataReWriters.get(mergeIndex).addDataEntities(new OrcStoreIncrementalDataEntity(new String[]{finalPathString + MAIN_FILE_EXTENSION}, finalPathString));
                this.hasMainFile[index] = false;
            }
        }

        for (OrcDataReWriter orcDataReWriter : this.orcDataReWriters) {
            orcDataReWriter.kill();
        }
        for (Future<Boolean> future : orcReWriteFutures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                logger.error("Error occurred while merging {}", e);
            }
        }
        this.service.shutdown();
    }

    private void mergeToFinal(int index) {
        String finalPathString = dataFilePrefix + fileNames.get(index);
        int mergeIndex = getMergerIndex(index, finalPathString);
        if (this.hasMainFile[index]) {
            orcDataReWriters.get(mergeIndex).addDataEntities(new OrcStoreIncrementalDataEntity(new String[]{finalPathString + CURR_FILE_EXTENSION, finalPathString + MAIN_FILE_EXTENSION}, finalPathString));
            this.hasMainFile[index] = false;
        } else {
            orcDataReWriters.get(mergeIndex).addDataEntities(new OrcStoreIncrementalDataEntity(new String[]{finalPathString + CURR_FILE_EXTENSION}, finalPathString));
        }


    }
}
