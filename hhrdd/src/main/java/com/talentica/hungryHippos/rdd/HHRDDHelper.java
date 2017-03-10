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

class HHRDDHelper {

    public final static String bucketCombinationToNodeNumbersMapFile =
            "bucketCombinationToNodeNumbersMap";
    public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";

  public static void initialize(String clientConfigPath) throws JAXBException, FileNotFoundException {
    ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, ClientConfig.class);
    String servers = clientConfig.getCoordinationServers().getServers();
    HungryHippoCurator.getInstance(servers);
  }

    public static String getActualPath(String path){
        return FileSystemContext.getRootDirectory()+ path;
    }

    public static Map<String,Long> readMetaData(String metadataLocation){
        Map<String,Long> fileNameToSizeWholeMap = new HashMap<>();
        File metadataFolder = new File(metadataLocation);
        if(metadataFolder.listFiles()!=null) {
            for (File file : metadataFolder.listFiles()) {
                try {
                    ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
                    Map<String, Long> fileNametoSizeMap = (Map<String, Long>) objectInputStream.readObject();
                    fileNameToSizeWholeMap.putAll(fileNametoSizeMap);
                    objectInputStream.close();

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }else{
            throw new RuntimeException(metadataLocation+" not a valid path");
        }
        System.out.println("No of Files : "+fileNameToSizeWholeMap.size());
        return fileNameToSizeWholeMap;
    }

  public static HHRDDInfo getHhrddInfo(String distributedPath)
      throws JAXBException, FileNotFoundException {
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
        String bucketCombinationToNodeNumbersMapFilePath = shardingFolderPath + File.separatorChar + bucketCombinationToNodeNumbersMapFile;
        String bucketToNodeNumberMapFilePath =
                shardingFolderPath + File.separatorChar + bucketToNodeNumberMapFile;
        Map<Integer, SerializedNode> nodIdToIp = new HashMap<>();
        for (SerializedNode serializedNode : nodes) {
          nodIdToIp.put(serializedNode.getId(), serializedNode);
        }
        Map<String,Long> fileNameToSizeWholeMap = readMetaData(metadataLocation);
        HHRDDInfoImpl hhrddInfo = new HHRDDInfoImpl(ShardingFileUtil.readFromFileBucketCombinationToNodeNumber(bucketCombinationToNodeNumbersMapFilePath),
                ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFilePath),fileNameToSizeWholeMap,
                context.getShardingDimensions(),nodIdToIp, shardingIndexes,dataDescription,directoryLocation);
        return hhrddInfo;

    }

    public static List<SerializedNode> getNodes(List<com.talentica.hungryhippos.config.cluster.Node> nodes) {
        List<SerializedNode> serializedNodes = new ArrayList<>();
        for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
            serializedNodes.add(new SerializedNode(node.getIdentifier(), node.getIp(),Integer.valueOf(node.getPort())));
        }
        return serializedNodes;
    }
}
