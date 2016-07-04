package com.talentica.hungryhippos.filesystem;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;

/**
 * This class is used for updating the Zookeeper FileSystem Node
 * Created by rajkishoreh on 29/6/16.
 */
public class ZookeeperFileSystemUpdater {

	private NodesManager nodesManager;

	public ZookeeperFileSystemUpdater(CoordinationServers coordinationServers) {
		nodesManager = NodesManagerContext.getNodesManagerInstance(coordinationServers);
	}


    /**
     * This method updates the zookeeper filesystem with the metadata of the file
     * @param filePath
     * @param nodeIp
     * @param dataFileZKNode
     * @param dataFileLocation
     * @param datafileSize
     * @throws Exception
     */
    public void updateZookeper(String filePath, String nodeIp, String dataFileZKNode, String dataFileLocation, long datafileSize) throws Exception {
        boolean flag= nodesManager.checkNodeExists(filePath + File.separator + nodeIp + File.separator + dataFileZKNode);

        long prevDataFileSize = 0;

        // if dataFileZKNode already exists, fetch the previous data file size and update data in dataFileZKNode
        if(flag){
            String prevDataFileData = (String) nodesManager.getObjectFromZKNode(filePath + File.separator + nodeIp + File.separator + dataFileZKNode);
            String[] dataArr = prevDataFileData.split(FileSystemConstants.FILE_PATHS_SEPARATOR);
            prevDataFileSize =  Long.parseLong(dataArr[1]);
            nodesManager.setObjectToZKNode(filePath + File.separator + nodeIp + File.separator + dataFileZKNode, dataFileLocation+","+datafileSize);
        }
        else{
            // if dataFileZKNode does not exist, create a new dataFileZKNode with details
            nodesManager.createPersistentNode(filePath + File.separator + nodeIp, new CountDownLatch(1), null);
            nodesManager.createPersistentNode(filePath + File.separator + nodeIp + File.separator + dataFileZKNode, new CountDownLatch(1), dataFileLocation+","+datafileSize);
        }

        //getting value already stored in the fileZKNodeValues
        String fileZKNodeValues = (String) nodesManager.getObjectFromZKNode(filePath);

        //updating the value in the fileZKNodeValues
        long currentSize = Long.parseLong(fileZKNodeValues)+datafileSize-prevDataFileSize;

        nodesManager.setObjectToZKNode(filePath, (datafileSize+""));

    }

    public static void main(String[] args) throws Exception {

		ZookeeperFileSystemUpdater zkCoordinator = new ZookeeperFileSystemUpdater(
				new ObjectFactory().createCoordinationServers());
        File file = new File("/home/rajkishoreh/out180mb/part-00007");
        zkCoordinator.updateZookeper("/filesystem/input", "172.29.3.18", "0", file.getPath(),file.length());
        file = new File("/home/rajkishoreh/out180mb/part-00000");
        zkCoordinator.updateZookeper("/filesystem/input", "172.29.3.18", "1", file.getPath(),file.length());
        file = new File("/home/rajkishoreh/out180mb/part-00001");
        zkCoordinator.updateZookeper("/filesystem/input", "172.29.3.18", "2", file.getPath(),file.length());
        file = new File("/home/rajkishoreh/out180mb/part-00002");
        zkCoordinator.updateZookeper("/filesystem/input", "172.29.3.18", "3", file.getPath(),file.length());

        file = new File("/home/rajkishoreh/out180mb/part-00003");
        zkCoordinator.updateZookeper("/filesystem/input", "localhost", "0", file.getPath(),file.length());
        file = new File("/home/rajkishoreh/out180mb/part-00004");
        zkCoordinator.updateZookeper("/filesystem/input", "localhost", "1", file.getPath(),file.length());
        file = new File("/home/rajkishoreh/out180mb/part-00005");
        zkCoordinator.updateZookeper("/filesystem/input", "localhost", "2", file.getPath(),file.length());
        file = new File("/home/rajkishoreh/out180mb/part-00006");
        zkCoordinator.updateZookeper("/filesystem/input", "localhost", "3", file.getPath(),file.length());
    }

}
