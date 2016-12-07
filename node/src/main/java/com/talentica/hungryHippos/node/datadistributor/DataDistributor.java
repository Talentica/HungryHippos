package com.talentica.hungryHippos.node.datadistributor;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.context.DataPublisherApplicationContext;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
import com.talentica.hungryHippos.sharding.*;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by rajkishoreh on 23/11/16.
 */
public class DataDistributor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataDistributor.class);

    private static int NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE;
    private static HungryHippoCurator curator = HungryHippoCurator.getInstance();


    private static Map<Integer, String> loadServers() throws Exception {

        LOGGER.info("Load the server form the configuration file");
        Map<Integer, String> servers = new HashMap<>();
        ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
        List<com.talentica.hungryhippos.config.cluster.Node> nodes = config.getNode();
        for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
            String server = node.getIp() + ServerUtils.COLON + node.getPort();
            servers.put(node.getIdentifier(), server);
        }
        LOGGER.info("There are {} servers", servers.size());
        return servers;
    }


    public static void distribute(String hhFilePath, String srcDataPath) throws Exception {
        NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE = Integer.valueOf(
                DataPublisherApplicationContext.getDataPublisherConfig().getNoOfAttemptsToConnectToNode());
        String BAD_RECORDS_FILE = srcDataPath + "_distributor.err";
        String shardingTablePath = getShardingTableLocation(hhFilePath);
        NewDataHandler.updateFilesIfRequired(shardingTablePath);
        ShardingApplicationContext context = new ShardingApplicationContext(shardingTablePath);
        Map<Integer, String> servers = loadServers();

        FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
        dataDescription.setKeyOrder(context.getShardingDimensions());
        byte[] buf = new byte[dataDescription.getSize()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
        String bucketCombinationPath =
                context.getBucketCombinationtoNodeNumbersMapFilePath();
        String keyToValueToBucketPath =
                context.getKeytovaluetobucketMapFilePath();
        Map<String, String> dataTypeMap =
                ShardingFileUtil.getDataTypeMap(context);

        String[] keyOrder = context.getShardingDimensions();
        Map<BucketCombination, Set<Node>> bucketCombinationNodeMap =
                ShardingFileUtil.readFromFileBucketCombinationToNodeNumber(bucketCombinationPath);

        HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
                ShardingFileUtil.readFromFileKeyToValueToBucket(keyToValueToBucketPath, dataTypeMap);
        BucketsCalculator bucketsCalculator =
                new BucketsCalculator(keyToValueToBucketMap, context);
        Map<Integer, OutputStream> targets = new HashMap<>();
        String fileIdToHHBasepath = CoordinationConfigUtil.getZkCoordinationConfigCache()
                .getZookeeperDefaultConfig().getFileidHhfsMapPath() + HungryHippoCurator.ZK_PATH_SEPERATOR;
        int fileId = fileIdToHHPathMap(fileIdToHHBasepath, hhFilePath);
        byte[] fileIdInBytes = ByteBuffer.allocate(4).putInt(fileId).array();
        String fileIdToHHpath = fileIdToHHBasepath + fileId;
        File srcFile = new File(srcDataPath);
        LOGGER.info("Size of file {} is {}", srcDataPath, srcFile.length());
        LOGGER.info("***CREATE SOCKET CONNECTIONS*** for {}", hhFilePath);

        Map<Integer, Socket> sockets = new HashMap<>();
        for (Integer nodeId : servers.keySet()) {
            String server = servers.get(nodeId);
            Socket socket = ServerUtils.connectToServer(server, NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE);
            sockets.put(nodeId, socket);
            BufferedOutputStream bos =
                    new BufferedOutputStream(sockets.get(nodeId).getOutputStream(), 8388608);
            targets.put(nodeId, bos);
            bos.write(fileIdInBytes);
            bos.flush();
            createNodeLink(fileIdToHHpath, nodeId);
        }

        String dataParserClassName =
                context.getShardingClientConfig().getInput().getDataParserConfig().getClassName();
        DataParser dataParser =
                (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
                        .newInstance(context.getConfiguredDataDescription());

        LOGGER.info("\n\tDISTRIBUTION OF DATA ACROSS THE NODES STARTED... for {}", hhFilePath);
        Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
                srcDataPath, dataParser);
        int lineNo = 0;
        FileWriter fileWriter = new FileWriter(BAD_RECORDS_FILE);
        fileWriter.openFile();

        int flushTriggerCount = 0;

        while (true) {
            DataTypes[] parts = null;
            try {
                parts = input.read();
            } catch (InvalidRowException e) {
                fileWriter.flushData(lineNo++, e);
                continue;
            }
            if (parts == null) {
                input.close();
                break;
            }
            Map<String, Bucket<KeyValueFrequency>> keyToBucketMap = new HashMap<>();

            for (int i = 0; i < keyOrder.length; i++) {
                String key = keyOrder[i];
                int keyIndex = Integer.parseInt(key.substring(3)) - 1;
                Object value = parts[keyIndex].clone();
                Bucket<KeyValueFrequency> bucket = bucketsCalculator.getBucketNumberForValue(key, value);
                keyToBucketMap.put(keyOrder[i], bucket);
            }

            for (int i = 0; i < dataDescription.getNumberOfDataFields(); i++) {
                Object value = parts[i].clone();
                dynamicMarshal.writeValue(i, value, byteBuffer);
            }
            BucketCombination bucketCombination = new BucketCombination(keyToBucketMap);
            Set<Node> nodes = bucketCombinationNodeMap.get(bucketCombination);
            Iterator<Node> nodeIterator = nodes.iterator();
            Node receivingNode = nodeIterator.next();
            targets.get(receivingNode.getNodeId()).write(buf);

            flushTriggerCount++;

            if (flushTriggerCount > 100000) {
                for (Integer nodeId : targets.keySet()) {
                    targets.get(nodeId).flush();
                }
                flushTriggerCount = 0;
            }
        }
        fileWriter.close();
        for (Integer nodeId : targets.keySet()) {
            targets.get(nodeId).flush();
            targets.get(nodeId).close();
            sockets.get(nodeId).close();
        }
        LOGGER.info("Waiting for data receiver signal for {}", hhFilePath);
        while (true) {
            List<String> children = curator.getChildren(fileIdToHHpath);
            if (children == null || children.isEmpty()) {
                curator.deletePersistentNodeIfExits(fileIdToHHpath);
                break;
            }
            if (NewDataHandler.checkIfFailed(hhFilePath)) {
                throw new RuntimeException("File distribution failed for " + hhFilePath + " in " + NodeInfo.INSTANCE.getIp());
            }
        }
        LOGGER.info("Data received successfully for {}", hhFilePath);
    }

    /**
     * Returns Sharding Table Location
     *
     * @return
     */
    public static String getShardingTableLocation(String hhFilePath) {
        String fileSystemBaseDirectory = FileSystemContext.getRootDirectory();
        String localDir = fileSystemBaseDirectory + hhFilePath;
        String shardingTableLocation =
                localDir + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        return shardingTableLocation;
    }

    private static int fileIdToHHPathMap(String path, String inputHHPath) {
        int i = 0;
        while (true) {
            try {
                curator.createPersistentNode(path + i, inputHHPath);
                return i;
            } catch (HungryHippoException e) {
                if (e instanceof HungryHippoException) {
                    i++;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * creates a node in zookeeper if file transfer started on a particular node.
     *
     * @param fileIdToHHBasepath
     * @param nodeId
     */
    public static void createNodeLink(String fileIdToHHBasepath, int nodeId) {

        curator.createPersistentNodeIfNotPresent(
                fileIdToHHBasepath + HungryHippoCurator.ZK_PATH_SEPERATOR + nodeId, "");

    }
}
