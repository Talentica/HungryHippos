package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryhippos.config.cluster.Node;

import java.util.*;

/**
 * Created by rajkishoreh on 30/9/16.
 */
public enum EndOfDataTracker {
    INSTANCE;

    private Map<Integer, Set<Integer>> fileIdToNodeSetMap;
    private String path;
    private Set<Integer> fileIdsToTrack;

    EndOfDataTracker() {
        fileIdToNodeSetMap = new HashMap<>();
        fileIdsToTrack = new HashSet<>();
        path = CoordinationConfigUtil.getZkCoordinationConfigCache()
                .getZookeeperDefaultConfig().getFileidHhfsMapPath();
    }

    public synchronized void addAllNodes(int fileId) {
        fileIdsToTrack.add(fileId);
        List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        Set nodesSet = new HashSet<>();
        for (Node node : nodes) {
            if (node.getIdentifier() != NodeInfo.INSTANCE.getIdentifier()) {
                nodesSet.add(node.getIdentifier());
            }
        }
        fileIdToNodeSetMap.put(fileId, nodesSet);
        String fileIdToNodeLink = getFileIdToNodeLink(fileId);
        ZkUtils.createZKNode(fileIdToNodeLink, null);
    }

    private String getFileIdToNodeLink(int fileId) {
        return path + ZkUtils.zkPathSeparator
                + fileId + ZkUtils.zkPathSeparator + NodeInfo.INSTANCE.getIdentifier();
    }

    public synchronized void addNode(int fileId, int nodeId) {

        if (fileIdToNodeSetMap.get(fileId) == null) {
            addAllNodes(fileId);
        } else {
            fileIdToNodeSetMap.get(fileId).add(nodeId);
        }
        String fileIdToNodeLink = getFileIdToNodeLink(fileId);
        ZkUtils.createZKNodeIfNotPresent(fileIdToNodeLink, null);

    }

    public synchronized void removeNode(int fileId, int nodeId) {
        if (fileIdToNodeSetMap.get(fileId) != null) {
            fileIdToNodeSetMap.get(fileId).remove(nodeId);
            if (fileIdToNodeSetMap.get(fileId).isEmpty()) {
                String fileIdToNodeLink = getFileIdToNodeLink(fileId);
                ZkUtils.deleteZKNode(fileIdToNodeLink);
            }
        }

    }

    public synchronized void remove(int fileId) {
        fileIdToNodeSetMap.remove(fileId);
        fileIdsToTrack.remove(fileId);
        if(checkFileIdEmptyForAllNodes(fileId)){
            ZkUtils.deleteZKNode(path+ZkUtils.zkPathSeparator+fileId);
        }
    }


    public synchronized void addNodeIfTracked(int fileId, int nodeId) {
        if (fileIdsToTrack.contains(fileId)) {
            addNode(fileId, nodeId);
        }
    }

    public synchronized boolean checkFileIdEmptyForAllNodes(int fileId) {
        return ZkUtils.getChildren(path+ZkUtils.zkPathSeparator+fileId).isEmpty();
    }
}
