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
package com.talentica.hungryHippos.node.uploaders;

import com.talentica.hungryHippos.node.datareceiver.ShardingResourceCache;
import com.talentica.hungryHippos.node.joiners.FileJoinCaller;
import com.talentica.hungryHippos.node.joiners.FirstStageNodeFileJoiner;
import com.talentica.hungryHippos.node.joiners.FirstStageNodeFileJoinerCaller;
import com.talentica.hungryHippos.node.joiners.NodeFileJoiner;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rajkishoreh on 15/5/17.
 */
public class NodeWiseFileUploader extends AbstractFileUploader {

    public NodeWiseFileUploader(CountDownLatch countDownLatch, String srcFolderPath, String destinationPath,
                              int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Node node,
                              Set<String> fileNames, String hhFilePath, String fileName) {
        super(countDownLatch,srcFolderPath,destinationPath,idx,dataInputStreamMap, socketMap,node,fileNames,hhFilePath, fileName);
    }

    @Override
    protected void sendTarFileLocal(String absolutePath){
        if(ShardingResourceCache.INSTANCE.getMaxFiles(getHhFilePath())>10000) {
            FirstStageNodeFileJoinerCaller.INSTANCE.addSrcFile(getHhFilePath(), absolutePath, (x, y) -> new FirstStageNodeFileJoiner(x, y));
        }else{
            FileJoinCaller.INSTANCE.addSrcFile(getHhFilePath(), absolutePath, (x, y) -> new NodeFileJoiner(x, y));
        }
    }

    @Override
    protected void createTar(String tarFilename) throws IOException {

    }

    @Override
    public void writeAppenderType(DataOutputStream dos) throws IOException {
        if(ShardingResourceCache.INSTANCE.getMaxFiles(getHhFilePath())>10000) {
            dos.writeInt(HungryHippoServicesConstants.DUAL_STAGE_NODE_DATA_APPENDER);
        }else{
            dos.writeInt(HungryHippoServicesConstants.NODE_DATA_APPENDER);
        }
    }
}
