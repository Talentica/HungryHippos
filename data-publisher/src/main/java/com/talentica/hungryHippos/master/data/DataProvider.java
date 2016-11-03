package com.talentica.hungryHippos.master.data;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
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
import com.talentica.hungryHippos.master.DataPublisherStarter;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.BucketsCalculator;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;

/**
 * {@code DataProvider} responsible for sending data across all the nodes.
 * 
 * @author debasishc
 * @since 24/9/15.
 */
public class DataProvider {
  private static int NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE;
  private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class.getName());
  private static Map<BucketCombination, Set<Node>> bucketCombinationNodeMap;
  private static Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<>();

  private static BucketsCalculator bucketsCalculator;
  private static String BAD_RECORDS_FILE;
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

  /**
   * publishes data to all nodes on the cluster from the {@value sourcePath} to
   * {@value destinationPath}.
   * 
   * @param dataParser
   * @param sourcePath
   * @param destinationPath
   * @throws Exception
   */
  public static void publishDataToNodes(DataParser dataParser, String sourcePath,
      String destinationPath) throws Exception {
    init();
    long start = System.currentTimeMillis();

    String fileIdToHHpath = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFileidHhfsMapPath() + HungryHippoCurator.ZK_PATH_SEPERATOR;
    Map<Integer, String> servers = loadServers();

    FieldTypeArrayDataDescription dataDescription =
        DataPublisherStarter.getContext().getConfiguredDataDescription();
    dataDescription.setKeyOrder(DataPublisherStarter.getContext().getShardingDimensions());
    byte[] buf = new byte[dataDescription.getSize()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    String bucketCombinationPath =
        DataPublisherStarter.getContext().getBucketCombinationtoNodeNumbersMapFilePath();
    String keyToValueToBucketPath =
        DataPublisherStarter.getContext().getKeytovaluetobucketMapFilePath();
    Map<String, String> dataTypeMap =
        ShardingFileUtil.getDataTypeMap(DataPublisherStarter.getContext());

    String[] keyOrder = DataPublisherStarter.getContext().getShardingDimensions();
    boolean keyOrderOne = keyOrder.length == 1;
    bucketCombinationNodeMap =
        ShardingFileUtil.readFromFileBucketCombinationToNodeNumber(bucketCombinationPath);

    keyToValueToBucketMap =
        ShardingFileUtil.readFromFileKeyToValueToBucket(keyToValueToBucketPath, dataTypeMap);
    bucketsCalculator =
        new BucketsCalculator(keyToValueToBucketMap, DataPublisherStarter.getContext());
    Map<Integer, OutputStream> targets = new HashMap<>();
    int fileId = fileIdToHHPathMap(fileIdToHHpath, destinationPath);
    byte[] fileIdInBytes = ByteBuffer.allocate(4).putInt(fileId).array();
    LOGGER.info("***CREATE SOCKET CONNECTIONS***");

    Map<Integer, Socket> sockets = new HashMap<>();
    for (Integer nodeId : servers.keySet()) {
      String server = servers.get(nodeId);
      Socket socket = ServerUtils.connectToServer(server, NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE);
      sockets.put(nodeId, socket);
      BufferedOutputStream bos =
          new BufferedOutputStream(sockets.get(nodeId).getOutputStream(), 8388608);
      targets.put(nodeId, bos);
      if (keyOrderOne) {
        bos.write(fileIdInBytes);
        bos.flush();
      }
      createNodeLink(fileIdToHHpath + fileId, nodeId);
    }

    LOGGER.info("\n\tPUBLISH DATA ACROSS THE NODES STARTED...");
    Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
        sourcePath, dataParser);
    long timeForEncoding = 0;
    long timeForLookup = 0;
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
      BucketCombination BucketCombination = new BucketCombination(keyToBucketMap);
      Set<Node> nodes = bucketCombinationNodeMap.get(BucketCombination);

      Iterator<Node> nodeIterator = nodes.iterator();
      Node receivingNode = nodeIterator.next();
      if (!keyOrderOne) {
        targets.get(receivingNode.getNodeId()).write(fileIdInBytes);
        for (int i = 1; i < keyOrder.length; i++) {
          byte nodeId = (byte) nodeIterator.next().getNodeId();
          targets.get(receivingNode.getNodeId()).write(nodeId);
        }

      }

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
      if (!keyOrderOne) {
        sendEndOfFileSignal(fileIdInBytes, buf, keyOrder, targets, nodeId);
      }
      targets.get(nodeId).flush();
      targets.get(nodeId).close();
      sockets.get(nodeId).close();
    }
    long end = System.currentTimeMillis();
    LOGGER.info("Time taken in ms: " + (end - start));
    LOGGER.info("Time taken in encoding: " + (timeForEncoding));
    LOGGER.info("Time taken in lookup: " + (timeForLookup));

  }

  /**
   * Updates file published successfully
   *
   * @param destinationPath
   */
  public static void updateFilePublishSuccessful(String destinationPath) {
    String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath() + destinationPath;
    String pathForSuccessNode =
        destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR + FileSystemConstants.DATA_READY;
    String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
        + FileSystemConstants.PUBLISH_FAILED;
    try {
      curator.deletePersistentNodeIfExits(pathForFailureNode);
      curator.createPersistentNodeIfNotPresent(pathForSuccessNode, "");
    } catch (HungryHippoException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }

  }

  private static void sendEndOfFileSignal(byte[] fileIdInBytes, byte[] buf, String[] keyOrder,
      Map<Integer, OutputStream> targets, Integer nodeId) throws IOException {
    targets.get(nodeId).write(fileIdInBytes);
    byte nodeIdByte = (byte) nodeId.intValue();
    for (int i = 1; i < keyOrder.length; i++) {
      targets.get(nodeId).write(nodeIdByte);
    }
    targets.get(nodeId).write(buf);
  }

  /**
   * creates a node in zookeeper if file transfer started on a particular node.
   * 
   * @param fileIdToHHpath
   * @param nodeId
   */
  public static void createNodeLink(String fileIdToHHpath, int nodeId) {
    try {
      curator.createPersistentNodeIfNotPresent(
          fileIdToHHpath + HungryHippoCurator.ZK_PATH_SEPERATOR + nodeId, "");
    } catch (HungryHippoException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  private static void init() throws FileNotFoundException, JAXBException {
    NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE = Integer.valueOf(
        DataPublisherApplicationContext.getDataPublisherConfig().getNoOfAttemptsToConnectToNode());
    BAD_RECORDS_FILE =
        DataPublisherStarter.getContext().getShardingClientConfig().getBadRecordsFileOut()
            + "_publisher.err";
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
}
