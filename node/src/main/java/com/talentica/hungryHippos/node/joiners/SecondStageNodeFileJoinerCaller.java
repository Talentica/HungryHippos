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

package com.talentica.hungryHippos.node.joiners;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by rajkishoreh on 6/7/17.
 */
public enum SecondStageNodeFileJoinerCaller {

    INSTANCE;

    public static Logger logger = LoggerFactory.getLogger(SecondStageNodeFileJoinerCaller.class);
    private Map<String, Map<Integer, Queue<String>>> hhFileToQueueMap = new HashMap<>();
    private Map<String, Queue<Future<Boolean>>> hhFileToCallableStatusMap = new HashMap<>();
    private Map<String, List<String>> hhFileToSrcFolderMap = new HashMap<>();

    public synchronized void addFiles(String firstStageOutputPath, String hhFilePath, File[] files) {
        Map<Integer, Queue<String>> srcFileQueueMap = hhFileToQueueMap.get(hhFilePath);
        if (srcFileQueueMap == null) {
            srcFileQueueMap = new HashMap<>();
            hhFileToQueueMap.put(hhFilePath, srcFileQueueMap);
        }
        Queue<Future<Boolean>> callableStatus = hhFileToCallableStatusMap.get(hhFilePath);
        if (callableStatus == null) {
            callableStatus = new ConcurrentLinkedQueue<>();
            hhFileToCallableStatusMap.put(hhFilePath, callableStatus);
            hhFileToSrcFolderMap.put(hhFilePath, new LinkedList<>());
        }
        hhFileToSrcFolderMap.get(hhFilePath).add(firstStageOutputPath);
        for (int i = 0; i < files.length; i++) {
            String fileName = files[i].getName();
            SecondStageNodeFileJoinerCaller.INSTANCE.addSrcFile(hhFilePath, Integer.valueOf(fileName), files[i].getAbsolutePath());
        }
    }

    private void addSrcFile(String hhFilePath, int fileId, String srcFilePath) {
        Map<Integer, Queue<String>> srcFileQueueMap = hhFileToQueueMap.get(hhFilePath);

        Queue<String> srcFileQueue = srcFileQueueMap.get(fileId);
        if (srcFileQueue == null) {
            srcFileQueue = new ConcurrentLinkedQueue<>();
            srcFileQueueMap.put(fileId, srcFileQueue);
        }

        while (!srcFileQueue.offer(srcFilePath)) ;
    }


    public synchronized boolean checkStatus(String hhFilePath) throws ExecutionException, InterruptedException {

        boolean result = true;
        try {
            if (hhFileToCallableStatusMap.get(hhFilePath) != null) {
                logger.info("Stage 2 file join started");
                Queue<Future<Boolean>> callableStatus = hhFileToCallableStatusMap.get(hhFilePath);
                Map<Integer, Queue<String>> srcFileQueueMap = hhFileToQueueMap.get(hhFilePath);
                ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                for (Map.Entry<Integer, Queue<String>> srcFileQueueEntry : srcFileQueueMap.entrySet()) {
                    Future<Boolean> future = executorService.submit(new SecondStageNodeFileJoiner(srcFileQueueEntry.getValue(), hhFilePath, srcFileQueueEntry.getKey()));
                    while (!callableStatus.offer(future)) ;
                }

                Queue<Future<Boolean>> futures = hhFileToCallableStatusMap.get(hhFilePath);
                Future<Boolean> future;
                while ((future = futures.poll()) != null) {
                    result = result && future.get();
                }
                logger.info("Stage 2 file join completed");
                executorService.shutdown();
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw e;
        } finally {
            hhFileToQueueMap.remove(hhFilePath);
            hhFileToCallableStatusMap.remove(hhFilePath);
            if (hhFileToCallableStatusMap.get(hhFilePath) != null) {
                for (String srcFolderPath : hhFileToSrcFolderMap.get(hhFilePath)) {
                    FileUtils.deleteQuietly(new File(srcFolderPath));
                }
            }
            System.gc();
        }
        return result;
    }

}
