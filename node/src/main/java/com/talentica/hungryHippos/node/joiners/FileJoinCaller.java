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
package com.talentica.hungryHippos.node.joiners;

import com.talentica.hungryHippos.node.DataDistributorStarter;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by rajkishoreh on 17/5/17.
 */
public enum FileJoinCaller {
    INSTANCE;

    private ExecutorService executorService = DataDistributorStarter.dataAppenderServices;
    private Map<String, Queue<String>> hhFileToQueueMap = new HashMap<>();
    private Map<String, List<Future<Boolean>>> hhFileToCallableStatusMap = new HashMap<>();

    public synchronized void addSrcFile(String hhFilePath, String srcFilePath, CallableGenerator callableGenerator) {
        Queue<String> srcFileQueue = hhFileToQueueMap.get(hhFilePath);
        List<Future<Boolean>> callableStatus = hhFileToCallableStatusMap.get(hhFilePath);
        if (srcFileQueue == null) {
            srcFileQueue = new ConcurrentLinkedQueue<>();
            hhFileToQueueMap.put(hhFilePath, srcFileQueue);
            callableStatus = new LinkedList<>();
            hhFileToCallableStatusMap.put(hhFilePath, callableStatus);

        }
        while (!srcFileQueue.offer(srcFilePath)) ;
        Future<Boolean> future = executorService.submit(callableGenerator.getCallable(srcFileQueue,hhFilePath));
        callableStatus.add(future);
    }

    public synchronized boolean checkStatus(String hhFilePath) throws ExecutionException, InterruptedException {
        boolean result = true;
        try {
            List<Future<Boolean>> futures = hhFileToCallableStatusMap.get(hhFilePath);
            if (futures != null) {
                Iterator<Future<Boolean>> futureIterator = futures.iterator();
                while (futureIterator.hasNext()) {
                    Future<Boolean> future = futureIterator.next();
                    result = result && future.get();
                }
            }
            return result;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw e;
        } finally {
            hhFileToQueueMap.remove(hhFilePath);
            hhFileToCallableStatusMap.remove(hhFilePath);
        }
    }

}
