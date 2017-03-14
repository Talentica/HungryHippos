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
