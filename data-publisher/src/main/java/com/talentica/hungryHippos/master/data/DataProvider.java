package com.talentica.hungryHippos.master.data;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

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
import com.talentica.hungryHippos.master.context.DataPublisherApplicationContext;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.BucketsCalculator;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.PathUtil;

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
    Properties prop = DataPublisherApplicationContext.getServerProperty().getProperties();
    int size = prop.keySet().size();
    for (int index = 0; index < size; index++) {
      String server = prop.getProperty(ServerUtils.PRIFIX_SERVER_NAME + ServerUtils.DOT + index);
      if (server == null)
        break;
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
        DataPublisherApplicationContext.getConfiguredDataDescription();
    dataDescription.setKeyOrder(DataPublisherApplicationContext.getShardingDimensions());
    byte[] buf = new byte[dataDescription.getSize()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);

    try (ObjectInputStream inBucketCombinationNodeMap =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.bucketCombinationToNodeNumbersMapFile))) {
      bucketCombinationNodeMap =
          (Map<BucketCombination, Set<Node>>) inBucketCombinationNodeMap.readObject();
    } catch (Exception exception) {
      LOGGER.error("Error occurred while publishing data on nodes.", exception);
      throw new RuntimeException(exception);
    }

    try (ObjectInputStream inBucketCombinationNodeMap =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + Sharding.keyToValueToBucketMapFile))) {
      keyToValueToBucketMap =
          (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) inBucketCombinationNodeMap
              .readObject();
      bucketsCalculator = new BucketsCalculator(keyToValueToBucketMap);
    } catch (Exception exception) {
      LOGGER.error("Error occurred while publishing data on nodes.", exception);
      throw new RuntimeException(exception);
    }

    OutputStream[] targets = new OutputStream[servers.length];
    LOGGER.info("***CREATE SOCKET CONNECTIONS***");

    for (int i = 0; i < servers.length; i++) {
      String server = servers[i];
      Socket socket = ServerUtils.connectToServer(server, NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE);
      targets[i] = new BufferedOutputStream(socket.getOutputStream(), 8388608);
    }

    LOGGER.info("\n\tPUBLISH DATA ACROSS THE NODES STARTED...");
    Reader input =
        new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
            DataPublisherApplicationContext.inputFile, dataParser);
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
      String dataPublishingNodeName =
          nodesManager.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.DATA_PUBLISHING_COMPLETED
              .getZKJobNode());
      CountDownLatch signal = new CountDownLatch(1);
      nodesManager.createPersistentNode(dataPublishingNodeName, signal);
      signal.await();
      LOGGER.info("DataPublishing completion notification created on zk node");
    } catch (Exception e) {
      LOGGER.info("Unable to connect the zk node due to {}", e);
    }

  }

  private static void init() {
    NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE =
        Integer.valueOf(DataPublisherApplicationContext.getProperty()
            .getValueByKey("no.of.attempts.to.connect.to.node").toString());
    BAD_RECORDS_FILE =
        DataPublisherApplicationContext.getProperty().getValueByKey("common.bad.records.file.out")
            + "_datapublish.err";
  }


  private int flushData(int lineNo, InvalidRowException e) {
    FileWriter.write("Error in line :: [" + (lineNo++) + "]  and columns(true are bad values) :: "
        + Arrays.toString(e.getColumns()) + " and row :: [" + e.getBadRow().toString() + "]");
    return lineNo;
  }
}
