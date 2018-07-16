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