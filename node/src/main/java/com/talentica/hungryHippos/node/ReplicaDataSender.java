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
public enum ReplicaDataSender {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaDataSender.class);
    private Map<Integer, BufferedOutputStream> nodeIdToBosMap;
    private WorkerReplicaDataSender workerReplicaDataSender;
    private Map<Integer, byte[][]> nodeIdToMemoryArraysMap;
    private Map<Integer, int[]> nodeIdToMemoryArrayLastByteIndex;
    private Map<Integer, Status[]> nodeIdToMemoryArrayStatusMap;
    private int memoryBlockCapacity;

    public enum Status {
        TRANSFER_IN_PROGRESS, ENABLE_BLOCK_WRITE, ENABLE_BLOCK_READ, LOCK_BLOCK_WRITE
    }

    ReplicaDataSender() {
        nodeIdToMemoryArraysMap = new HashMap<>();
        List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        memoryBlockCapacity = 2 * 1024 * 1024;
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
        workerReplicaDataSender = new WorkerReplicaDataSender();
        workerReplicaDataSender.start();
    }

    private void initialize(int memoryArraySize, Node node) {
        int nodeId = node.getIdentifier();
        byte[][] memoryArrayBlock = new byte[memoryArraySize][];
        nodeIdToMemoryArraysMap.put(nodeId, memoryArrayBlock);
        this.nodeIdToMemoryArrayLastByteIndex.put(nodeId, new int[memoryArraySize]);
        Status[] statuses = new Status[memoryArraySize];
        for (int j = 0; j < memoryArraySize; j++) {
            nodeIdToMemoryArraysMap.get(nodeId)[j] = new byte[memoryBlockCapacity];
            statuses[j] = Status.ENABLE_BLOCK_WRITE;
        }
        this.nodeIdToMemoryArrayStatusMap.put(nodeId, statuses);
        addBufferedOutputStream(nodeId);
    }

    private void addBufferedOutputStream(int nodeId) {
        Socket socket = NodeConnectionPool.INSTANCE.getNodeConnectionMap().get(nodeId);
        BufferedOutputStream bos = null;
        try {
            bos = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.info(e.toString());
            throw new RuntimeException(e);
        }
        nodeIdToBosMap.put(nodeId, bos);
    }

    private class WorkerReplicaDataSender extends Thread {

        private boolean keepAlive = true;

        @Override
        public void run() {
            LOGGER.info("Started publishing replica data");
            while (keepAlive) {
                publishReplicaData();
            }
            publishReplicaData();

            LOGGER.info("Completed publishing replica data");
        }

        /**
         * Publishes the replica data to the all the other nodes
         *
         * @throws IOException
         */
        private void publishReplicaData() {
            for (Map.Entry<Integer, Status[]> entry : nodeIdToMemoryArrayStatusMap.entrySet()) {
                Status[] statuses = entry.getValue();
                int nodeId = entry.getKey();
                for (int i = 0; i < statuses.length; i++) {
                    if (statuses[i] == Status.ENABLE_BLOCK_READ) {
                        statuses[i] = Status.TRANSFER_IN_PROGRESS;
                        try {
                            nodeIdToBosMap.get(nodeId).write(nodeIdToMemoryArraysMap.get(nodeId)[i],
                                    0, nodeIdToMemoryArrayLastByteIndex.get(nodeId)[i]);
                            nodeIdToBosMap.get(nodeId).flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                            LOGGER.error(e.toString());
                            NodeConnectionPool.INSTANCE.reConnect(nodeId);
                            addBufferedOutputStream(nodeId);
                        }
                        nodeIdToMemoryArrayLastByteIndex.get(nodeId)[i] = 0;
                        statuses[i] = Status.ENABLE_BLOCK_WRITE;
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
