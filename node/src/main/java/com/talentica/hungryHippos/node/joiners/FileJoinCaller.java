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
