package com.talentica.hungryHippos.node.service;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.context.DataPublisherApplicationContext;
import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.BucketsCalculator;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

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
    //HHFileStatusCoordinator.updateFilesIfRequired(shardingTablePath);
    ShardingApplicationContext context = new ShardingApplicationContext(shardingTablePath);
    FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    byte[] buf = new byte[dataDescription.getSize()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    String keyToValueToBucketPath = context.getKeytovaluetobucketMapFilePath();
    String keyToBucketToNodePath = context.getBuckettoNodeNumberMapFilePath();
    String bucketCombinationPath = context.getBucketCombinationtoNodeNumbersMapFilePath();
    Map<String, String> dataTypeMap = ShardingFileUtil.getDataTypeMap(context);

    String[] keyOrder = context.getShardingDimensions();

    HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
        ShardingFileUtil.readFromFileKeyToValueToBucket(keyToValueToBucketPath, dataTypeMap);
    HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> keyToBucketToNodetMap =
        ShardingFileUtil.readFromFileBucketToNodeNumber(keyToBucketToNodePath);
    Map<BucketCombination, Set<Node>> bucketCombinationNodeMap =
        ShardingFileUtil.readFromFileBucketCombinationToNodeNumber(bucketCombinationPath);
    BucketsCalculator bucketsCalculator = new BucketsCalculator(keyToValueToBucketMap, context);


    File srcFile = new File(srcDataPath);

    String dataParserClassName =
        context.getShardingClientConfig().getInput().getDataParserConfig().getClassName();
    DataParser dataParser = (DataParser) Class.forName(dataParserClassName)
        .getConstructor(DataDescription.class).newInstance(context.getConfiguredDataDescription());


    LOGGER.info("\n\tDISTRIBUTION OF DATA ACROSS THE NODES STARTED... for {}", hhFilePath);

    if (srcFile.exists()) {
      HHFileMapper hhFileMapper = new HHFileMapper(hhFilePath, context, dataDescription,
          keyToBucketToNodetMap, bucketCombinationNodeMap, keyOrder);
      Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
          srcDataPath, dataParser);
      int lineNo = 0;
      FileWriter fileWriter = new FileWriter(BAD_RECORDS_FILE);
      fileWriter.openFile();

      int[] buckets = new int[keyOrder.length];
      int maxBucketSize =
          Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize());
      int index;
      Bucket<KeyValueFrequency> bucket;
      String key;
      int keyIndex;
      DataTypes[] parts;
      while (true) {
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

        for (int i = 0; i < keyOrder.length; i++) {
          key = keyOrder[i];
          keyIndex = context.assignShardingIndexByName(key);
          bucket = bucketsCalculator.getBucketNumberForValue(key, parts[keyIndex]);
          buckets[i] = bucket.getId();
        }

        index = indexCalculator(buckets, maxBucketSize);

        for (int i = 0; i < dataDescription.getNumberOfDataFields(); i++) {
          dynamicMarshal.writeValue(i, parts[i], byteBuffer);
        }
        hhFileMapper.storeRow(index, buf);

      }
      srcFile.delete();
      hhFileMapper.sync();
      fileWriter.close();
    }
    System.gc();
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



  private static int indexCalculator(int[] values, int base) {
    int index = 0;
    for (int i = 0; i < values.length; i++) {
      index = index + values[i] * power(base, i);
    }
    return index;
  }

  private static int power(int x, int pow) {
    int value = 1;
    for (int i = 0; i < pow; i++) {
      value = value * x;
    }
    return value;
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
