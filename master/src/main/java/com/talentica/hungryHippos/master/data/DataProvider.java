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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.marshaling.Reader;
import com.talentica.hungryHippos.utility.server.ServerUtils;

/**
 * Created by debasishc on 24/9/15.
 */
public class DataProvider {

	private static final int NO_OF_ATTEMPTS_TO_CONNECT_TO_NODE = Integer
			.valueOf(Property.getPropertyValue("no.of.attempts.to.connect.to.node").toString());

	private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class.getName());
	private static Map<BucketCombination, Set<Node>> bucketCombinationNodeMap;
	private static Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = new HashMap<>();
	
	
	
	private static String[] loadServers(NodesManager nodesManager) throws Exception {
		LOGGER.info("Load the server form the configuration file");
		ArrayList<String> servers = new ArrayList<>();
		Object obj = nodesManager.getConfigFileFromZNode(Property.SERVER_CONF_FILE);
		ZKNodeFile serverConfig = (obj == null) ? null : (ZKNodeFile) obj;
		Properties prop = serverConfig.getFileData();
		int size = prop.keySet().size();
		for (int index = 0 ; index < size ; index++) {
			System.out.println();
			servers.add(prop.getProperty(ServerUtils.PRIFIX_SERVER_NAME + ServerUtils.DOT +index));
		}
		LOGGER.info("There are {} servers", servers.size());
		return servers.toArray(new String[servers.size()]);
	}

	@SuppressWarnings({ "unchecked" })
	public static void publishDataToNodes(NodesManager nodesManager) throws Exception {

		long start = System.currentTimeMillis();

		String[] servers = loadServers(nodesManager);
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		CommonUtil.setDataDescription(dataDescription);
		dataDescription.setKeyOrder(Property.getKeyOrder());
		byte[] buf = new byte[dataDescription.getSize()];
		ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);

		try (ObjectInputStream inBucketCombinationNodeMap = new ObjectInputStream(
				new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH
						+ Sharding.bucketCombinationToNodeNumbersMapFile))) {
			bucketCombinationNodeMap = (Map<BucketCombination, Set<Node>>) inBucketCombinationNodeMap.readObject();
		} catch (Exception exception) {
			LOGGER.error("Error occurred while publishing data on nodes.", exception);
			throw new RuntimeException(exception);
		}

		try (ObjectInputStream inBucketCombinationNodeMap = new ObjectInputStream(
				new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH
						+ Sharding.keyToValueToBucketMapFile))) {
			keyToValueToBucketMap = (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) inBucketCombinationNodeMap
					.readObject();
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
		Reader input = new com.talentica.hungryHippos.utility.marshaling.FileReader(
				Property.getProperties().getProperty("input.file"));
		input.setNumFields(9);
		input.setMaxsize(25);

		long timeForEncoding = 0;
		long timeForLookup = 0;

		while (true) {
			MutableCharArrayString[] parts = input.read();
			if (parts == null) {
				input.close();
				break;
			}
			MutableCharArrayString value1 = parts[0];
			MutableCharArrayString value2 = parts[1];
			MutableCharArrayString value3 = parts[2];
			MutableCharArrayString value4 = parts[3];
			MutableCharArrayString value5 = parts[4];
			MutableCharArrayString value6 = parts[5];
			double value7 = Double.parseDouble(parts[6].toString());
			double value8 = Double.parseDouble(parts[7].toString());
			MutableCharArrayString value9 = parts[8];

			Map<String, Bucket<KeyValueFrequency>> keyToBucketMap = new HashMap<>();
			String[] keyOrder = Property.getKeyOrder();

			for (int i = 0; i < keyOrder.length; i++) {
				Bucket<KeyValueFrequency> bucket = keyToValueToBucketMap.get(keyOrder[i]).get(parts[i]);
				keyToBucketMap.put(keyOrder[i], bucket);
			}
			dynamicMarshal.writeValueString(0, value1, byteBuffer);
			dynamicMarshal.writeValueString(1, value2, byteBuffer);
			dynamicMarshal.writeValueString(2, value3, byteBuffer);
			dynamicMarshal.writeValueString(3, value4, byteBuffer);
			dynamicMarshal.writeValueString(4, value5, byteBuffer);
			dynamicMarshal.writeValueString(5, value6, byteBuffer);
			dynamicMarshal.writeValueDouble(6, value7, byteBuffer);
			dynamicMarshal.writeValueDouble(7, value8, byteBuffer);
			dynamicMarshal.writeValueString(8, value9, byteBuffer);
			BucketCombination BucketCombination = new BucketCombination(keyToBucketMap);
			Set<Node> nodes = bucketCombinationNodeMap.get(BucketCombination);
			for (Node node : nodes) {
				targets[node.getNodeId()].write(buf);
			}

		}
		for (int j = 0; j < targets.length; j++) {
			targets[j].flush();
			targets[j].close();
		}
		long end = System.currentTimeMillis();
		LOGGER.info("Time taken in ms: " + (end - start));
		LOGGER.info("Time taken in encoding: " + (timeForEncoding));
		LOGGER.info("Time taken in lookup: " + (timeForLookup));
	}

}