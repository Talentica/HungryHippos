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
package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.utility.FileSystemConstants;

/**
 * Created by rajkishoreh on 21/11/16.
 */
public class HHFileStatusCoordinator {


    public static void updateFailure(String hhFilePath, String cause) {
        HungryHippoCurator curator = HungryHippoCurator.getInstance();
        String destinationPathNode = CoordinationConfigUtil.getFileSystemPath() + hhFilePath;
        String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
                + FileSystemConstants.PUBLISH_FAILED;
        try {
            if(checkIfFailed(hhFilePath)){
                cause =  cause + "\n" + curator.getZnodeData(pathForFailureNode);
            }
        } catch (HungryHippoException e) {
            e.printStackTrace();
        }
        curator.createPersistentNodeIfNotPresent(pathForFailureNode, NodeInfo.INSTANCE.getIp()+" : "+ cause);
    }

    public static boolean checkIfFailed(String hhFilePath) throws HungryHippoException {
        HungryHippoCurator curator = HungryHippoCurator.getInstance();
        String destinationPathNode = CoordinationConfigUtil.getFileSystemPath() + hhFilePath;
        String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
                + FileSystemConstants.PUBLISH_FAILED;
        return  curator.checkExists(pathForFailureNode);
    }



}
