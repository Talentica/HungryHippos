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
package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.rdd.utility.JaxbUtil;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * The Class HHRDDHelper.
 */
class HHRDDHelper {

    /** The Constant bucketCombinationToNodeNumbersMapFile. */
    public final static String bucketCombinationToNodeNumbersMapFile =
            "bucketCombinationToNodeNumbersMap";
    
    /** The Constant bucketToNodeNumberMapFile. */
    public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";

  /**
   * Initialize.
   *
   * @param clientConfigPath the client config path
   * @throws JAXBException the JAXB exception
   * @throws FileNotFoundException the file not found exception
   */
  public static void initialize(String clientConfigPath) throws JAXBException, FileNotFoundException {
    ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, ClientConfig.class);
    String servers = clientConfig.getCoordinationServers().getServers();
    HungryHippoCurator.getInstance(servers);
  }

    /**
     * Gets the actual path.
     *
     * @param path the path
     * @return the actual path
     */
    public static String getActualPath(String path){
        return FileSystemContext.getRootDirectory()+ path;
    }

    /**
     * Read meta data.
     *
     * @param metadataLocation the metadata location
     * @return the map
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static Map<String,Long> readMetaData(String metadataLocation) throws IOException {
        Map<String,Long> fileNameToSizeWholeMap = new HashMap<>();
        File metadataFolder = new File(metadataLocation);
        if(metadataFolder.listFiles()!=null) {
            for (File file : metadataFolder.listFiles()) {
                ObjectInputStream objectInputStream =null;
                try {
                    objectInputStream = new ObjectInputStream(new FileInputStream(file));
                    Map<String, Long> fileNametoSizeMap = (Map<String, Long>) objectInputStream.readObject();
                    fileNameToSizeWholeMap.putAll(fileNametoSizeMap);


                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Corrupted Metadata : "+e);
                } finally {
                    if(objectInputStream!=null)
                        try {
                            objectInputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                }
            }
        }else{
            throw new RuntimeException(metadataLocation+" not a valid path");
        }
        System.out.println("No of Files : "+fileNameToSizeWholeMap.size());
        return fileNameToSizeWholeMap;
    }

  /**
   * Gets the hhrdd info.
   *
   * @param distributedPath the distributed path
   * @return the hhrdd info
   * @throws JAXBException the JAXB exception
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static HHRDDInfo getHhrddInfo(String distributedPath)
          throws JAXBException, IOException {
        String shardingFolderPath = FileSystemContext.getRootDirectory() + distributedPath
                + File.separator + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        String directoryLocation = FileSystemContext.getRootDirectory() + distributedPath
                + File.separator + FileSystemContext.getDataFilePrefix();
        String metadataLocation = FileSystemContext.getRootDirectory() + distributedPath
                + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME;
        ShardingApplicationContext context = new ShardingApplicationContext(shardingFolderPath);
        FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
        ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
        List<SerializedNode> nodes = getNodes(clusterConfig.getNode());
        int[] shardingIndexes = new int[context.getShardingDimensions().length];
        for (int i = 0; i < shardingIndexes.length; i++) {
            shardingIndexes[i]= i;
        }
        
        String bucketToNodeNumberMapFilePath =
                shardingFolderPath + File.separatorChar + bucketToNodeNumberMapFile;
        Map<Integer, SerializedNode> nodIdToIp = new HashMap<>();
        for (SerializedNode serializedNode : nodes) {
          nodIdToIp.put(serializedNode.getId(), serializedNode);
        }
        Map<String,Long> fileNameToSizeWholeMap = readMetaData(metadataLocation);
        HHRDDInfoImpl hhrddInfo = new HHRDDInfoImpl(ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFilePath),fileNameToSizeWholeMap,
                context.getShardingDimensions(),nodIdToIp, shardingIndexes,dataDescription,directoryLocation);
        return hhrddInfo;

    }

    /**
     * Gets the nodes.
     *
     * @param nodes the nodes
     * @return the nodes
     */
    public static List<SerializedNode> getNodes(List<com.talentica.hungryhippos.config.cluster.Node> nodes) {
        List<SerializedNode> serializedNodes = new ArrayList<>();
        for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
            serializedNodes.add(new SerializedNode(node.getIdentifier(), node.getIp(),Integer.valueOf(node.getPort())));
        }
        return serializedNodes;
    }
}
