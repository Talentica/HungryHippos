package com.talentica.hungryHippos.tools;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.tools.clients.FileExtractionClient;
import com.talentica.hungryhippos.config.cluster.Node;

import java.io.IOException;
import java.util.List;

/**
 * This class is for extracting files in nodes
 * Created by rajkishoreh on 22/8/16.
 */
public class FilesExtractor {

    /**
     * Extract the zipped file in all nodes
     * @param filePathForExtraction
     * @throws IOException
     */
    public static void extractFileInNodes(String filePathForExtraction) throws IOException {
        FileExtractionClient fileExtractionClient =new FileExtractionClient(filePathForExtraction);
        List<Node> nodes = CoordinationApplicationContext.getZkClusterConfigCache().getNode();
        for(Node node:nodes){
            fileExtractionClient.sendRequest(node.getIp());
        }
    }
}
