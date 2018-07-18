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

import com.talentica.hungryhippos.filesystem.orc.OrcReader;
import com.talentica.hungryhippos.filesystem.orc.OrcWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

/**
 * Created by rajkishoreh on 7/5/18.
 */

public class OrcFileMerger {

    public static int REWRITE_THRESHOLD =  4 * 1024 * 1024;
    private static Logger logger = LoggerFactory.getLogger(OrcFileMerger.class);
    private static Semaphore rewriteSemaphore = new Semaphore(50);
    private static Semaphore mergeSemaphore = new Semaphore(50);


    public static void rewriteData(Path destinationPath, List<Path> pathList, boolean deleteMergedPathsFlag, boolean needPermission) throws IOException {
        File destinationFile = new File(destinationPath.toString());
        if (destinationFile.exists()) pathList.add(destinationPath);

        String fileName = UUID.randomUUID().toString();
        Path intermediatePath = new Path(destinationPath.getParent().toString() + File.separator + fileName);
        List<Path> mergedPaths = new LinkedList<>();
        OrcWriter orcWriter = null;
        if(needPermission){
            while(!rewriteSemaphore.tryAcquire()){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }

        try {
            for (Path path : pathList) {
                try (OrcReader orcReader = new OrcReader(new Configuration(), path, null)) {
                    if (orcWriter == null) {
                        orcWriter = new OrcWriter(orcReader.getSchema(), orcReader.getColumnNames(), intermediatePath);
                    }
                    while (orcReader.hasNext()) {
                        orcWriter.write(orcReader.nextCompleteRow());
                    }
                    mergedPaths.add(path);
                }
            }
            orcWriter.flush();
            orcWriter.close();
        }finally {
            if(needPermission){
                rewriteSemaphore.release();
            }
        }

        logger.info("Rewriting data of:"+pathList+ " Merged Paths:" + mergedPaths);
        if (deleteMergedPathsFlag) {
            deleteMergedPaths(destinationPath, pathList);
        }

        new File(intermediatePath.getParent().toString()+File.separator + "." + fileName + ".crc").delete();
        if (destinationFile.exists()) destinationFile.delete();
        new File(intermediatePath.toString()).renameTo(destinationFile);
    }

    private static void deleteMergedPaths(Path destinationPath, List<Path> pathList) {
        for (Path path : pathList) {
            if (!path.toString().equals(destinationPath.toString()))
                new File(path.toString()).delete();
        }
    }

    public static void mergeFiles(Path destinationPath, List<Path> pathList, boolean deleteMergedPathsFlag, boolean needPermission) throws IOException {
        File destinationFile = new File(destinationPath.toString());
        if(destinationFile.exists()) pathList.add(destinationPath);
        String fileName = UUID.randomUUID().toString();
        Path intermediatePath = new Path(destinationPath.getParent().toString() + File.separator + fileName);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(new Configuration());
        if(needPermission){
            while(!mergeSemaphore.tryAcquire()){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }

        try {
            List<Path> mergedPaths = OrcFile.mergeFiles(intermediatePath, writerOptions, pathList);
            logger.info("Merging data of:"+pathList+ " Merged:" + mergedPaths);
        }finally {
            if(needPermission){
                mergeSemaphore.release();
            }
        }
        if (deleteMergedPathsFlag) {
            deleteMergedPaths(destinationPath, pathList);
        }

        new File(intermediatePath.getParent().toString()+File.separator + "." + fileName + ".crc").delete();
        if(destinationFile.exists()) destinationFile.delete();
        new File(intermediatePath.toString()).renameTo(destinationFile);

    }

}
