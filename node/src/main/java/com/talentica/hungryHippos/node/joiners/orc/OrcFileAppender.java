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

import com.talentica.hungryHippos.node.datareceiver.SynchronousFolderDeleter;
import com.talentica.hungryHippos.storage.IncrementalDataEntity;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

public class OrcFileAppender implements Callable<Boolean> {
    private final ExecutorService worker;
    private final Queue<Future<Boolean>> futures;
    private Queue<IncrementalDataEntity> dataEntities;
    private Semaphore semaphore;


    public OrcFileAppender(ExecutorService worker, Queue<Future<Boolean>> futures, Queue<IncrementalDataEntity> dataEntities, Semaphore semaphore) {
        this.dataEntities = dataEntities;
        this.semaphore = semaphore;
        this.worker = worker;
        this.futures = futures;
    }

    @Override
    public Boolean call() throws Exception {
        if (!semaphore.tryAcquire()) {
            return true;
        }
        try {
            IncrementalDataEntity dataEntity = dataEntities.poll();
            if (dataEntity == null) {
                return true;
            }
            String destPath = dataEntity.getDestPath();
            File destFile = new File(destPath);
            if (!destFile.exists()) destFile.mkdirs();
            Path finalFilePath = new Path(destPath + FileSystemConstants.ORC_MAIN_FILE_NAME);
            Path deltaFilePath = new Path(destPath + FileSystemConstants.ORC_DELTA_FILE_NAME);
            appendFileData(dataEntity, finalFilePath, deltaFilePath);
            while ((dataEntity = dataEntities.poll())!=null) {
                appendFileData(dataEntity, finalFilePath, deltaFilePath);
            }
        } finally {
            semaphore.release();
        }
        if(dataEntities.peek()!=null){
            futures.offer(worker.submit(new OrcFileAppender(worker,futures,dataEntities,semaphore)));
        }

        return true;
    }

    private void appendFileData(IncrementalDataEntity dataEntity, Path finalFilePath, Path deltaFilePath) throws IOException {
        Path srcPath = new Path(dataEntity.getSrcPath());
        List<Path> pathList = new LinkedList<>();
        pathList.add(srcPath);
        if (new File(dataEntity.getSrcPath()).length() > OrcFileMerger.REWRITE_THRESHOLD) {
            OrcFileMerger.mergeFiles(finalFilePath, pathList,false);
        } else {
            OrcFileMerger.mergeFiles(deltaFilePath, pathList,false);
            if (new File(deltaFilePath.toString()).length() > OrcFileMerger.REWRITE_THRESHOLD) {
                List<Path> deltaList = new LinkedList<>();
                deltaList.add(deltaFilePath);
                Path intermediatePath = new Path(deltaFilePath.getParent().toString()+File.separator+UUID.randomUUID().toString());
                OrcFileMerger.rewriteData(intermediatePath,deltaList,true);
                List<Path> intermediateList = new LinkedList<>();
                intermediateList.add(intermediatePath);
                OrcFileMerger.mergeFiles(finalFilePath, intermediateList,true);
            }
        }
        dataEntity.updateComplete();
        File parentFile = new File(dataEntity.getSrcPath()).getParentFile();
        String[] siblings = parentFile.list();
        if (siblings == null || siblings.length == 0) {
            SynchronousFolderDeleter.INSTANCE.deleteEmptyFolder(parentFile);
        }
    }

}
