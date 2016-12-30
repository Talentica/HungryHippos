package com.talentica.hungryHippos.rdd.utility;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.SerializedNode;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class HHRDDHelper {

    /**
     *
     */
    private static final long serialVersionUID = -3090261268542999403L;
    public final static String bucketCombinationToNodeNumbersMapFile =
            "bucketCombinationToNodeNumbersMap";
    public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";

    public static HHRDDConfigSerialized getHhrddConfigSerialized(String distributedPath, String clientConfigPath) throws JAXBException, FileNotFoundException {

        ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, ClientConfig.class);
        String servers = clientConfig.getCoordinationServers().getServers();
        HungryHippoCurator.getInstance(servers);
        String shardingFolderPath = FileSystemContext.getRootDirectory() + distributedPath
                + HungryHippoCurator.ZK_PATH_SEPERATOR + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        String directoryLocation = FileSystemContext.getRootDirectory() + distributedPath
                + HungryHippoCurator.ZK_PATH_SEPERATOR + FileSystemContext.getDataFilePrefix();
        ShardingApplicationContext context = new ShardingApplicationContext(shardingFolderPath);
        FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
        ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
        List<SerializedNode> nodes = getNodes(clusterConfig.getNode());
        int[] shardingIndexes = new int[context.getShardingDimensions().length];
        for (int i = 0; i < shardingIndexes.length; i++) {
            shardingIndexes[i]= i;
        }
        HHRDDConfigSerialized hhrddConfigSerialized =
                new HHRDDConfigSerialized(dataDescription.getSize(),
                        context.getShardingDimensions(),shardingIndexes,directoryLocation,
                        shardingFolderPath, nodes,
                        context.getConfiguredDataDescription());
        return hhrddConfigSerialized;

    }

    public static List<SerializedNode> getNodes(List<com.talentica.hungryhippos.config.cluster.Node> nodes) {
        List<SerializedNode> serializedNodes = new ArrayList<>();
        for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
            serializedNodes.add(new SerializedNode(node.getIdentifier(), node.getIp()));
        }
        return serializedNodes;
    }

    public static Map<BucketCombination, Set<Node>> populateBucketCombinationToNodeNumber(HHRDDConfigSerialized hipposRDDConf) {
        String bucketCombinationToNodeNumbersMapFilePath = hipposRDDConf.getShardingFolderPath()
                + File.separatorChar + bucketCombinationToNodeNumbersMapFile;
        return ShardingFileUtil
                .readFromFileBucketCombinationToNodeNumber(bucketCombinationToNodeNumbersMapFilePath);
    }

    public static HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> populateBucketToNodeNumber(HHRDDConfigSerialized hipposRDDConf) {
        String bucketToNodeNumberMapFilePath =
                hipposRDDConf.getShardingFolderPath() + File.separatorChar + bucketToNodeNumberMapFile;
        return ShardingFileUtil.readFromFileBucketToNodeNumber(bucketToNodeNumberMapFilePath);
    }

    public static String generateKeyForHHRDD(Job job, int[] sortedShardingIndexes) {
        boolean keyCreated = false;
        Integer[] jobDimensions = job.getDimensions();
        StringBuilder jobShardingDimensions = new StringBuilder();
        for (int i = 0; i < sortedShardingIndexes.length; i++) {
            for (int j = 0; j < jobDimensions.length; j++) {
                if(jobDimensions[j]==sortedShardingIndexes[i]){
                    keyCreated= true;
                    jobShardingDimensions.append(sortedShardingIndexes[i]).append("-");
                }
            }
        }
        if(!keyCreated){
            jobShardingDimensions.append(sortedShardingIndexes[0]);
        }
        return jobShardingDimensions.toString();
    }

}
