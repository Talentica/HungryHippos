package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rajkishoreh on 22/9/16.
 */
public enum BossReplicaDataSender {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(BossReplicaDataSender.class);
    private Map<Integer,BufferedOutputStream> nodeIdToBosMap;
    private WorkerReplicaDataSender workerReplicaDataSender;
    private Map<Integer,byte[][]> nodeIdToMemoryArraysMap;
    private Map<Integer,Integer> nodeIdToCurrentMemoryArrayIndexMap;
    private Map<Integer,Integer> nodeIdToCurrentMemoryArrayLastByteIndexMap;
    private Map<Integer,int[]> nodeIdToMemoryArrayLastByteIndex;
    private Map<Integer,Status[]> nodeIdToMemoryArrayStatusMap;
    private int memoryBlockCapacity;

    public enum Status {
        SENDING_BLOCK, ENABLE_BLOCK_WRITE, ENABLE_BLOCK_READ
    }

    BossReplicaDataSender()
            throws IOException {
        nodeIdToMemoryArraysMap = new HashMap<>();
        nodeIdToCurrentMemoryArrayLastByteIndexMap = new HashMap<>();
        nodeIdToCurrentMemoryArrayIndexMap = new HashMap<>();
        List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        memoryBlockCapacity = 10*1024*1024;
        nodeIdToBosMap = new HashMap<>();
        this.nodeIdToMemoryArrayLastByteIndex = new HashMap<>();
        this.nodeIdToMemoryArrayStatusMap = new HashMap<>();
        int memoryArraySize = 10;
        for (Node node : nodes) {
            String nodeIp = node.getIp();
            if (!nodeIp.equals(NodeInfo.INSTANCE.getIp())) {
                initialize(memoryArraySize, node);
            }
        }
        workerReplicaDataSender = new WorkerReplicaDataSender(nodeIdToBosMap, nodeIdToMemoryArraysMap, nodeIdToMemoryArrayLastByteIndex,nodeIdToMemoryArrayStatusMap);
        workerReplicaDataSender.start();
    }

    private void initialize(int memoryArraySize, Node node) throws IOException {
        int nodeId = node.getIdentifier();
        nodeIdToCurrentMemoryArrayLastByteIndexMap.put(nodeId, 0);
        byte[][] memoryArrayBlock = new byte[memoryArraySize][];
        nodeIdToMemoryArraysMap.put(nodeId, memoryArrayBlock);
        this.nodeIdToMemoryArrayLastByteIndex.put(nodeId,new int[memoryArraySize]);
        Status[] statuses = new Status[memoryArraySize];
        for (int j = 0; j < memoryArraySize; j++) {
            nodeIdToMemoryArraysMap.get(nodeId)[j] = new byte[memoryBlockCapacity];
            statuses[j] = Status.ENABLE_BLOCK_WRITE;
        }
        this.nodeIdToMemoryArrayStatusMap.put(nodeId,statuses);
        nodeIdToCurrentMemoryArrayIndexMap.put(nodeId, 0);
        Socket socket = NodeConnectionPool.INSTANCE.getNodeConnectionMap().get(nodeId);
        BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());
        nodeIdToBosMap.put(nodeId,bos);
    }

    private class WorkerReplicaDataSender extends Thread {
        private Map<Integer,BufferedOutputStream> nodeIdToBosMap;
        private boolean keepAlive = true;
        private Map<Integer,byte[][]> nodeIdToMemoryArraysMap;
        private Map<Integer,int[]> nodeIdToMemoryArrayLastByteIndex;
        private Map<Integer,Status[]> nodeIdToMemoryArrayStatusMap;

        public WorkerReplicaDataSender(Map<Integer, BufferedOutputStream> nodeIdToBosMap, Map<Integer, byte[][]> nodeIdToMemoryArraysMap, Map<Integer, int[]> nodeIdToMemoryArrayLastByteIndex,
                                       Map<Integer, Status[]> nodeIdToMemoryArrayStatusMap) {
            this.nodeIdToBosMap = nodeIdToBosMap;
            this.nodeIdToMemoryArraysMap = nodeIdToMemoryArraysMap;
            this.nodeIdToMemoryArrayLastByteIndex = nodeIdToMemoryArrayLastByteIndex;
            this.nodeIdToMemoryArrayStatusMap = nodeIdToMemoryArrayStatusMap;
        }

        @Override
        public void run() {
            LOGGER.info("Started publishing replica data");
            while (keepAlive) {
                try {
                    publishReplicaData();
                } catch (IOException e) {
                    LOGGER.error(e.toString());
                    throw new RuntimeException(e);
                }
            }
            try {
                publishReplicaData();
            } catch (IOException e) {
                LOGGER.error(e.toString());
            }
            LOGGER.info("Completed publishing replica data");
        }

        /**
         * Publishes the replica data to the all the other nodes
         *
         * @throws IOException
         */
        private void publishReplicaData() throws IOException {
            for(Map.Entry<Integer,Status[]> entry:this.nodeIdToMemoryArrayStatusMap.entrySet()){
                Status[] statuses = entry.getValue();
                int nodeId = entry.getKey();
                for (int i = 0; i < statuses.length; i++) {
                    if(statuses[i]==Status.ENABLE_BLOCK_READ){
                        statuses[i]=Status.SENDING_BLOCK;
                        this.nodeIdToBosMap.get(nodeId).write(this.nodeIdToMemoryArraysMap.get(nodeId)[i],
                                0, this.nodeIdToMemoryArrayLastByteIndex.get(nodeId)[i]);
                        this.nodeIdToBosMap.get(nodeId).flush();
                        statuses[i]=Status.ENABLE_BLOCK_WRITE;
                    }

                }
            }
        }


        public void kill() {
            keepAlive = false;
        }
    }

    public Map<Integer, byte[][]> getNodeIdToMemoryArraysMap() {
        return nodeIdToMemoryArraysMap;
    }

    public Map<Integer, Integer> getNodeIdToCurrentMemoryArrayIndexMap() {
        return nodeIdToCurrentMemoryArrayIndexMap;
    }

    public Map<Integer, Integer> getNodeIdToCurrentMemoryArrayLastByteIndexMap() {
        return nodeIdToCurrentMemoryArrayLastByteIndexMap;
    }

    public int getMemoryBlockCapacity() {
        return memoryBlockCapacity;
    }

    public Map<Integer, int[]> getNodeIdToMemoryArrayLastByteIndex() {
        return nodeIdToMemoryArrayLastByteIndex;
    }

    public Map<Integer, Status[]> getNodeIdToMemoryArrayStatusMap() {
        return nodeIdToMemoryArrayStatusMap;
    }


}
