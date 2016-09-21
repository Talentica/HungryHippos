package com.talentica.hungryHippos.master.data;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.context.DataPublisherApplicationContext;
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
import com.talentica.hungryhippos.config.cluster.ClusterConfig;

/**
 * Created by debasishc on 24/9/15.
 */
public class DataProvider {
  private static int NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE;
  private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class.getName());
  private static Map<BucketCombination, Set<Node>> bucketCombinationNodeMap;
  private static Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<>();

  private static BucketsCalculator bucketsCalculator;
  private static String BAD_RECORDS_FILE;
  private static Random random;

  private static Map<Integer,String> loadServers(NodesManager nodesManager) throws Exception {
    LOGGER.info("Load the server form the configuration file");
    Map<Integer,String> servers = new HashMap<Integer,String>();
    ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
    List<com.talentica.hungryhippos.config.cluster.Node> nodes = config.getNode();
    for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
      String server = node.getIp() + ServerUtils.COLON + node.getPort();
      servers.put(node.getIdentifier(),server);
    }
    LOGGER.info("There are {} servers", servers.size());
    return servers;
    /*ArrayList<String> servers = new ArrayList<>();
    ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
    List<com.talentica.hungryhippos.config.cluster.Node> nodes = config.getNode();
    for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
      String server = node.getIp() + ServerUtils.COLON + node.getPort();
      servers.add(server);
    }
    LOGGER.info("There are {} servers", servers.size());
    return servers.toArray(new String[servers.size()]);*/
  }

  public static void publishDataToNodes(NodesManager nodesManager, DataParser dataParser,
      String sourcePath, String destinationPath) throws Exception {
    init();
    long start = System.currentTimeMillis();
    Map<Integer,String> servers = loadServers(nodesManager);
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
    Map<String, String> dataTypeMap = ShardingFileUtil.getDataTypeMap(DataPublisherStarter.getContext());
    
    String[] keyOrder = DataPublisherStarter.getContext().getShardingDimensions();
    bucketCombinationNodeMap =
        ShardingFileUtil.readFromFileBucketCombinationToNodeNumber(bucketCombinationPath);
    keyToValueToBucketMap = ShardingFileUtil.readFromFileKeyToValueToBucket(keyToValueToBucketPath,dataTypeMap);
    bucketsCalculator = new BucketsCalculator(keyToValueToBucketMap,DataPublisherStarter.getContext());
    Map<Integer,OutputStream> targets = new HashMap<>();
    LOGGER.info("***CREATE SOCKET CONNECTIONS***");

    Map<Integer,Socket> sockets = new HashMap<>();
    DataOutputStream dos = null;
    byte[] destinationPathInBytes = destinationPath.getBytes(Charset.defaultCharset());
    int destinationPathLength = destinationPathInBytes.length;
    
    for(Integer nodeId : servers.keySet()){
      String server = servers.get(nodeId);
      Socket socket = ServerUtils.connectToServer(server, NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE);
      sockets.put(nodeId, socket);
      BufferedOutputStream bos = new BufferedOutputStream(sockets.get(nodeId).getOutputStream(), 8388608);
      targets.put(nodeId, bos);
      dos = new DataOutputStream(sockets.get(nodeId).getOutputStream());
      dos.writeInt(destinationPathLength);
      dos.flush();
      targets.get(nodeId).write(destinationPathInBytes);
      targets.get(nodeId).write((byte)1);
      targets.get(nodeId).flush();
    }

    LOGGER.info("\n\tPUBLISH DATA ACROSS THE NODES STARTED...");
    Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
        sourcePath, dataParser);
    long timeForEncoding = 0;
    long timeForLookup = 0;
    int lineNo = 0;
    FileWriter fileWriter = new FileWriter(BAD_RECORDS_FILE);
    fileWriter.openFile();
    random = ThreadLocalRandom.current();
    byte[] nextNodesInfo = new byte[DataPublisherStarter.getContext().getShardingDimensions().length-1];
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
      int randomNodeIndex = random.nextInt(nodes.size());
      int setIndex = 0;
      int nextNodesInfoIndex = 0;
      Node randomNode = null;
      for(Node node:nodes){
        if(setIndex==randomNodeIndex){
          randomNode = node;
        }else{
          nextNodesInfo[nextNodesInfoIndex] = (byte)node.getNodeId();
          nextNodesInfoIndex++;
        }
        setIndex++;
      }
      targets.get(randomNode.getNodeId()).write(nextNodesInfo);
      targets.get(randomNode.getNodeId()).write(buf);
      flushTriggerCount++;
      if(flushTriggerCount>100000){
        for(Integer nodeId : targets.keySet()){
          targets.get(nodeId).flush();
        }
        flushTriggerCount =0;
      }
    }
    fileWriter.close();
    for(Integer nodeId : targets.keySet()){
      targets.get(nodeId).flush();
      targets.get(nodeId).close();
      sockets.get(nodeId).close();
    }
    long end = System.currentTimeMillis();
    LOGGER.info("Time taken in ms: " + (end - start));
    LOGGER.info("Time taken in encoding: " + (timeForEncoding));
    LOGGER.info("Time taken in lookup: " + (timeForLookup));

  }

  private static void init() throws FileNotFoundException, JAXBException {
    NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE = Integer.valueOf(DataPublisherApplicationContext
        .getDataPublisherConfig().getNoOfAttemptsToConnectToNode());
    BAD_RECORDS_FILE =
        DataPublisherStarter.getContext().getShardingServerConfig().getBadRecordsFileOut() + "_publisher.err";
  }
}
