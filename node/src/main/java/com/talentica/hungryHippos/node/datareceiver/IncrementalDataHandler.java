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

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.joiners.SnappyFileAppender;
import com.talentica.hungryHippos.storage.IncrementalDataEntity;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public enum IncrementalDataHandler {
    INSTANCE;
    private static final Logger logger = LoggerFactory.getLogger(IncrementalDataHandler.class);
    private Map<String, Queue<Future<Boolean>>> hhfileToFutures;
    private Map<String, AtomicInteger> hhfileToCounter;
    private Map<String, Map<Integer,Map<String, Semaphore>>> dataFileToSemaphore;
    private Map<String, Map<String, Queue<IncrementalDataEntity>>> hhFileToDataFileQueue;
    private List<Object> lockObjects;
    private int noOfLocks = 100;
    private Map<String, ExecutorService> workers;
    private Map<String, IncrementalDataStatusChecker> statusCheckers;
    private Map<Integer, Queue<IncrementalDataEntity>> uploaderQueue;
    private Map<Integer, Node> nodeMap;
    private int selfNodeId;
    private int workloadLimit = 10*Runtime.getRuntime().availableProcessors();


    IncrementalDataHandler() {
        selfNodeId = NodeInfo.INSTANCE.getIdentifier();
        hhFileToDataFileQueue = new HashMap<>();
        hhfileToFutures = new HashMap<>();
        dataFileToSemaphore = new HashMap<>();
        hhfileToCounter = new HashMap<>();
        lockObjects = new ArrayList<>();
        for (int i = 0; i < noOfLocks; i++) {
            lockObjects.add(new Object());
        }
        workers = new HashMap<>();
        statusCheckers = new HashMap<>();
        nodeMap = new HashMap<>();
        List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        uploaderQueue = new HashMap<>();
        for (Node node : nodes) {
            nodeMap.put(node.getIdentifier(), node);
            if (node.getIdentifier() != selfNodeId) {
                uploaderQueue.put(node.getIdentifier(), new ConcurrentLinkedQueue<>());
            }
        }
    }

    public boolean checkAvailable() {
        if (hhFileToDataFileQueue.values().stream().map(
                x -> x.values().stream().map(y -> y.size()).reduce((a, b) -> a + b).orElse(0))
                .reduce((c, d) -> c + d).orElse(0) > workloadLimit) {
            return false;
        }
        return true;
    }


    public void initialize(String hhfile, String destFolderPath, Collection<String> fileNames, Map<String, int[]>  fileToNodeMap){
        Object lockObj = lockObjects.get((hhfile.hashCode() % noOfLocks + noOfLocks) % noOfLocks);
        synchronized (lockObj) {
            Map<String, Queue<IncrementalDataEntity>> dataFileMap = hhFileToDataFileQueue.get(hhfile);
            if (dataFileMap == null) {
                dataFileMap = new HashMap<>();
                hhFileToDataFileQueue.put(hhfile, dataFileMap);
                hhfileToFutures.put(hhfile, new ConcurrentLinkedQueue<>());
                dataFileToSemaphore.put(hhfile, new HashMap<>());
                for(Integer nodeId:nodeMap.keySet()){
                    dataFileToSemaphore.get(hhfile).put(nodeId,new HashMap<>());
                }
                for(String fileName : fileNames){
                    String destPath = destFolderPath+fileName+ FileSystemConstants.SNAPPY_EXTENSION;
                    dataFileMap.put(destPath, new ConcurrentLinkedQueue<>());
                    for(Integer nodeId:fileToNodeMap.get(fileName)){
                        dataFileToSemaphore.get(hhfile).get(nodeId).put(destPath, new Semaphore(1));
                    }

                }
                hhfileToCounter.put(hhfile, new AtomicInteger());
                statusCheckers.put(hhfile, new IncrementalDataStatusChecker(hhfileToFutures.get(hhfile)));
                int noOfProcessors = Runtime.getRuntime().availableProcessors();
                workers.put(hhfile, Executors.newFixedThreadPool(2 * noOfProcessors));
                statusCheckers.get(hhfile).start();
            }

            hhfileToCounter.get(hhfile).incrementAndGet();
        }
    }

    public void register(String hhfile, String srcPath, String destPath, int[] nodeIds) {
        Queue<IncrementalDataEntity> incrementalDataEntities = hhFileToDataFileQueue.get(hhfile).get(destPath);
        IncrementalDataEntity dataEntity = new IncrementalDataEntity(srcPath, destPath, new AtomicInteger(nodeIds.length));
        incrementalDataEntities.offer(dataEntity);
        for (Integer nodeId : nodeIds) {
            if (selfNodeId == nodeId) {
                hhfileToFutures.get(hhfile).offer(workers.get(hhfile).submit(new SnappyFileAppender(incrementalDataEntities, dataFileToSemaphore.get(hhfile).get(nodeId).get(destPath))));
            } else {
                Node node = nodeMap.get(nodeId);
                Queue<IncrementalDataEntity> dataEntities = uploaderQueue.get(nodeId);
                dataEntities.offer(dataEntity);
                hhfileToFutures.get(hhfile).offer(workers.get(hhfile).submit(new IncrementalDataUploader(dataEntities, node.getIp(), node.getPort(), dataFileToSemaphore.get(hhfile).get(nodeId))));
            }
        }

    }

    public void completedRegister(String hhfile){
        hhfileToCounter.get(hhfile).decrementAndGet();
    }

    public boolean checkStatus(String hhfile) throws ExecutionException, InterruptedException {

        Map<String, Queue<IncrementalDataEntity>> dataFileMap = hhFileToDataFileQueue.get(hhfile);
        if (dataFileMap == null) return true;
        while (hhfileToCounter.get(hhfile).get() != 0) {
            Thread.sleep(2000);
        }
        while (!hhfileToFutures.get(hhfile).isEmpty()) {
            Thread.sleep(2000);
        }
        Object lockObj = lockObjects.get((hhfile.hashCode() % noOfLocks + noOfLocks) % noOfLocks);
        synchronized (lockObj) {
            logger.info("Acquired lock on {}", hhfile);
            while (hhfileToCounter.get(hhfile).get() != 0) {
                Thread.sleep(2000);
            }

            statusCheckers.get(hhfile).kill();
            statusCheckers.get(hhfile).join();
            boolean status = true;
            if (!statusCheckers.get(hhfile).isSuccess()) {
                status= false;
            }
            if(status){
                while (!hhfileToFutures.get(hhfile).isEmpty()) {
                    Future<Boolean> future = hhfileToFutures.get(hhfile).poll();
                    if(!future.isDone()){
                        while(!hhfileToFutures.get(hhfile).offer(future));
                    }else if(!future.get()){
                        status= false;
                        break;
                    }
                }
            }

            workers.get(hhfile).shutdown();
            clear(hhfile);
            logger.info("Release lock on {}", hhfile);
            return status;
        }
    }


    private void clear(String hhfile) {
        dataFileToSemaphore.remove(hhfile);
        hhfileToFutures.remove(hhfile);
        hhFileToDataFileQueue.remove(hhfile);
        hhfileToCounter.remove(hhfile);
        workers.remove(hhfile);
        statusCheckers.remove(hhfile);
    }
}
