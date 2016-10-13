package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by rajkishoreh on 30/9/16.
 */
public enum EndOfDataTracker {
    INSTANCE;

    private static Logger LOGGER = LoggerFactory.getLogger(EndOfDataTracker.class);
    private List<Node> nodeList;
    private Map<Integer,Map<Integer,Integer>> fileToDimensionIdxToSignalCount;

    EndOfDataTracker() {
        fileToDimensionIdxToSignalCount = new HashMap<>();
        nodeList = CoordinationConfigUtil.getZkClusterConfigCache().getNode();

    }

    public synchronized  int getCountDown(int fileId, int dimensionIdx){
        Map<Integer,Integer> dimensionIdxToSignalCount = fileToDimensionIdxToSignalCount.get(fileId);
        if(dimensionIdxToSignalCount==null){
            dimensionIdxToSignalCount = new HashMap<>();
            fileToDimensionIdxToSignalCount.put(fileId,dimensionIdxToSignalCount);
        }
        if(dimensionIdxToSignalCount.get(dimensionIdx)==null){
            dimensionIdxToSignalCount.put(dimensionIdx,nodeList.size()-1);
        }
        int count = dimensionIdxToSignalCount.get(dimensionIdx);
        count--;
        dimensionIdxToSignalCount.put(dimensionIdx,count);
       return count;
    }

    public synchronized void remove(int fileId){
        fileToDimensionIdxToSignalCount.remove(fileId);
        String hhFileIdNodePath = getHHFileIdNodePath(fileId);
        ZkUtils.deleteZKNode(hhFileIdNodePath);
        String hhFileIdPath= getHHFileIdPath(fileId);
        if(ZkUtils.checkIfNodeExists(hhFileIdPath)){
            List<String> children = ZkUtils.getChildren(hhFileIdPath);
            if(children==null || children.isEmpty()){
                ZkUtils.deleteZKNode(hhFileIdPath);
            }
        }

    }

    /**
     * Updates file published successfully
     *
     * @param destinationPath
     */
    public synchronized void updateFilePublishSuccessful(String destinationPath) {
        String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
                .getZookeeperDefaultConfig().getFilesystemPath() + destinationPath;
        String pathForSuccessNode = destinationPathNode +ZkUtils.zkPathSeparator + FileSystemConstants.DATA_READY;
        String pathForFailureNode = destinationPathNode + ZkUtils.zkPathSeparator+ FileSystemConstants.PUBLISH_FAILED;
        ZkUtils.deleteZKNode(pathForFailureNode);
        ZkUtils.createZKNodeIfNotPresent(pathForSuccessNode, "");
    }

    private String getHHFileIdNodePath(int fileId) {
        String hhFileIdPath = getHHFileIdPath(fileId);
        return hhFileIdPath+ ZkUtils.zkPathSeparator+ NodeInfo.INSTANCE.getIdentifier();
    }


    private String getHHFileIdPath(int fileId) {
        String hhFileIdRootPath = CoordinationConfigUtil.getZkCoordinationConfigCache().getZookeeperDefaultConfig().getFileidHhfsMapPath();
        return hhFileIdRootPath+ ZkUtils.zkPathSeparator+fileId;
    }
}
