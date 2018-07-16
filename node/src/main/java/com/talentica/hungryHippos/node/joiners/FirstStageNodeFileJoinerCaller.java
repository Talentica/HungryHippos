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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by rajkishoreh on 31/7/17.
 */
public enum FirstStageNodeFileJoinerCaller {
    INSTANCE;

    public static Logger logger = LoggerFactory.getLogger(FileJoinCaller.class);
    private Map<String, Queue<String>> hhFileToQueueMap = new HashMap<>();
    private Map<String, List<Future<Boolean>>> hhFileToCallableStatusMap = new HashMap<>();
    private Map<String, List<CallableGenerator>> hhFileToCallableGeneratorMap = new HashMap<>();


    public synchronized void addSrcFile(String hhFilePath, String srcFilePath, CallableGenerator callableGenerator) {
        Queue<String> srcFileQueue = hhFileToQueueMap.get(hhFilePath);
        List<CallableGenerator> callableGeneratorList = hhFileToCallableGeneratorMap.get(hhFilePath);
        if (srcFileQueue == null) {
            srcFileQueue = new ConcurrentLinkedQueue<>();
            hhFileToQueueMap.put(hhFilePath, srcFileQueue);
            callableGeneratorList = new LinkedList<>();
            hhFileToCallableGeneratorMap.put(hhFilePath, callableGeneratorList);
        }
        while (!srcFileQueue.offer(srcFilePath)) ;
        callableGeneratorList.add(callableGenerator);
    }

    public synchronized boolean checkStatus(String hhFilePath) throws ExecutionException, InterruptedException {
        boolean result = true;
        try {
            if (hhFileToQueueMap.get(hhFilePath) != null) {
                logger.info("Stage 1 file join started");
                Queue<String> srcFileQueue = hhFileToQueueMap.get(hhFilePath);
                List<CallableGenerator> callableGeneratorList = hhFileToCallableGeneratorMap.get(hhFilePath);
                List<Future<Boolean>> callableStatus = new LinkedList<>();
                hhFileToCallableStatusMap.put(hhFilePath, callableStatus);
                ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                for (CallableGenerator callableGenerator : callableGeneratorList) {
                    Future<Boolean> future = executorService.submit(callableGenerator.getCallable(srcFileQueue, hhFilePath));
                    callableStatus.add(future);
                }

                List<Future<Boolean>> futures = hhFileToCallableStatusMap.get(hhFilePath);
                Iterator<Future<Boolean>> futureIterator = futures.iterator();
                while (futureIterator.hasNext()) {
                    Future<Boolean> future = futureIterator.next();
                    result = result && future.get();
                }
                logger.info("Stage 1 file join completed");
                executorService.shutdown();

            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw e;
        } finally {
            hhFileToQueueMap.remove(hhFilePath);
            hhFileToCallableStatusMap.remove(hhFilePath);
            hhFileToCallableGeneratorMap.remove(hhFilePath);
            System.gc();
        }
        return result && SecondStageNodeFileJoinerCaller.INSTANCE.checkStatus(hhFilePath);
    }
}
