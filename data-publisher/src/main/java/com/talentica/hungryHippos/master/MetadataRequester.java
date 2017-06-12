package com.talentica.hungryHippos.master;

import com.talentica.hungryhippos.config.cluster.Node;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 7/4/17.
 */

public class MetadataRequester implements Callable<Boolean>{

    private int nodeId;
    private List<Node> nodes;
    private String hhFilePath;

    public MetadataRequester(int nodeId, List<Node> nodes, String hhFilePath) {
        this.nodeId = nodeId;
        this.nodes = nodes;
        this.hhFilePath = hhFilePath;
    }

    @Override
    public Boolean call() throws Exception {
        DataPublisherStarter.updateNodeMetaData(hhFilePath, nodes.get(nodeId));
        return true;
    }
}