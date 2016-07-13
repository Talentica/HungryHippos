package com.talentica.hungryHippos.master.data;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.BucketsCalculator;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.ShardingTable;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

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

  private static String[] loadServers(NodesManager nodesManager) throws Exception {
    LOGGER.info("Load the server form the configuration file");
    ArrayList<String> servers = new ArrayList<>();
    CoordinationConfig config = CoordinationApplicationContext.getZkCoordinationConfigCache();
    List<com.talentica.hungryhippos.config.coordination.Node> nodes =
        config.getClusterConfig().getNode();
    for (com.talentica.hungryhippos.config.coordination.Node node : nodes) {
      String server = node.getIp() + ServerUtils.COLON + node.getPort();
      servers.add(server);
    }
    LOGGER.info("There are {} servers", servers.size());
    return servers.toArray(new String[servers.size()]);
  }

  @SuppressWarnings({"unchecked"})
  public static void publishDataToNodes(NodesManager nodesManager, DataParser dataParser)
      throws Exception {
    init();
    long start = System.currentTimeMillis();
    String[] servers = loadServers(nodesManager);
    FieldTypeArrayDataDescription dataDescription =
        CoordinationApplicationContext.getConfiguredDataDescription();
    dataDescription.setKeyOrder(CoordinationApplicationContext.getShardingDimensions());
    byte[] buf = new byte[dataDescription.getSize()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    ShardingTable shardingTable = new ShardingTable();
    bucketCombinationNodeMap = shardingTable.readBucketCombinationToNodeNumbersMap();
    keyToValueToBucketMap = shardingTable.readKeyToValueToBucketMap();
    bucketsCalculator = new BucketsCalculator(keyToValueToBucketMap);
    OutputStream[] targets = new OutputStream[servers.length];
    LOGGER.info("***CREATE SOCKET CONNECTIONS***");

    for (int i = 0; i < servers.length; i++) {
      String server = servers[i];
      Socket socket = ServerUtils.connectToServer(server, NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE);
      targets[i] = new BufferedOutputStream(socket.getOutputStream(), 8388608);
    }

    LOGGER.info("\n\tPUBLISH DATA ACROSS THE NODES STARTED...");
    Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
        CoordinationApplicationContext.getZkCoordinationConfigCache().getInputFileConfig()
            .getInputFileName(),
        dataParser);
    long timeForEncoding = 0;
    long timeForLookup = 0;
    int lineNo = 0;
    FileWriter.openFile(BAD_RECORDS_FILE);
    while (true) {
      DataTypes[] parts = null;
      try {
        parts = input.read();
      } catch (InvalidRowException e) {
        FileWriter.flushData(lineNo++, e);
        continue;
      }
      if (parts == null) {
        input.close();
        break;
      }

      Map<String, Bucket<KeyValueFrequency>> keyToBucketMap = new HashMap<>();
      String[] keyOrder = CoordinationApplicationContext.getShardingDimensions();

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
      for (Node node : nodes) {
        targets[node.getNodeId()].write(buf);
      }

    }
    FileWriter.close();
    for (int j = 0; j < targets.length; j++) {
      targets[j].flush();
      targets[j].close();
    }
    long end = System.currentTimeMillis();
    LOGGER.info("Time taken in ms: " + (end - start));
    LOGGER.info("Time taken in encoding: " + (timeForEncoding));
    LOGGER.info("Time taken in lookup: " + (timeForLookup));
    try {
      String dataPublishingNodeName = nodesManager
          .buildAlertPathByName(CommonUtil.ZKJobNodeEnum.DATA_PUBLISHING_COMPLETED.getZKJobNode());
      CountDownLatch signal = new CountDownLatch(1);
      nodesManager.createPersistentNode(dataPublishingNodeName, signal);
      signal.await();
      LOGGER.info("DataPublishing completion notification created on zk node");
    } catch (Exception e) {
      LOGGER.info("Unable to connect the zk node due to {}", e);
    }

  }

  private static void init() throws FileNotFoundException, JAXBException {
    NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE = Integer.valueOf(CoordinationApplicationContext
        .getZkCoordinationConfigCache().getCommonConfig().getNoOfAttemptsToConnectToNode());
    BAD_RECORDS_FILE = CoordinationApplicationContext.getZkCoordinationConfigCache()
        .getCommonConfig().getBadRecordsFileOut();
  }
}
