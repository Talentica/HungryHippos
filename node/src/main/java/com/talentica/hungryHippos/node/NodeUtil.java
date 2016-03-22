package com.talentica.hungryHippos.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.PathUtil;

@SuppressWarnings("unchecked")
public class NodeUtil {

	private static final String nodeIdFile = "nodeId";

	private static final Logger LOGGER = LoggerFactory.getLogger(NodeUtil.class);

	private static Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;

	private static Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = null;
	

	static {
		try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
				new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH
						+ Sharding.keyToValueToBucketMapFile))) {
			keyToValueToBucketMap = (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) inKeyValueNodeNumberMap
					.readObject();
		} catch (IOException | ClassNotFoundException e) {
			LOGGER.info("Unable to read keyValueNodeNumberMap. Please put the file in current directory");
		}

		try (ObjectInputStream bucketToNodeNumberMapInputStream = new ObjectInputStream(
				new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH
						+ Sharding.bucketToNodeNumberMapFile))) {
			bucketToNodeNumberMap = (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) bucketToNodeNumberMapInputStream
					.readObject();
		} catch (IOException | ClassNotFoundException e) {
			LOGGER.info("Unable to read bucketToNodeNumberMap. Please put the file in current directory");
		}
	}

	public static final Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
		return keyToValueToBucketMap;
	}

	public static final Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
		return bucketToNodeNumberMap;
	}

	/**
	 * Read the file nodeId which contains nodeId value.
	 * 
	 * @return NodeId
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public static int getNodeId() throws IOException {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(
					new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH + nodeIdFile)));
			String line = in.readLine();
			return Integer.parseInt(line);
		} catch (IOException exception) {
			LOGGER.info("Unable to read the file for NODE ID. Exiting..");
			throw exception;
		}
	}
	
	public static void createTrieBucketToNodeNumberMap(
			Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
			NodesManager nodesManager) throws IOException {
		String buildPath = nodesManager.buildConfigPath("bucketToNodeNumberMap");
		
		for (String keyName : bucketToNodeNumberMap.keySet()) {
			String keyNodePath = buildPath + PathUtil.FORWARD_SLASH + keyName;
			nodesManager.createPersistentNode(keyNodePath, null);
			LOGGER.info("Path {} is created",keyNodePath);
			Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = bucketToNodeNumberMap
					.get(keyName);
			for (Bucket<KeyValueFrequency> bucketKey : bucketToNodeMap.keySet()) {
				String buildBucketNodePath = keyNodePath;
				nodesManager.createPersistentNode(buildBucketNodePath, null);
				String buildBucketKeyPath = buildBucketNodePath + PathUtil.FORWARD_SLASH + bucketKey.toString();
				nodesManager.createPersistentNode(buildBucketKeyPath, null);
				String buildNodePath = buildBucketKeyPath + PathUtil.FORWARD_SLASH + bucketToNodeMap.get(bucketKey).toString();
				nodesManager.createPersistentNode(buildNodePath, null);
				//nodesManager.createPersistentNode(buildBucketNodePath, null);
				LOGGER.info("Path {} is created",buildBucketNodePath);
			}
		}
	}
			
	public static void createTrieKeyToValueToBucketMap(
			Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap,
			NodesManager nodesManager) throws IOException {
		String buildPath = nodesManager
				.buildConfigPath("keyToValueToBucketMap");
		for (String keyName : keyToValueToBucketMap.keySet()) {
			String keyNodePath = buildPath + PathUtil.FORWARD_SLASH + keyName;
			nodesManager.createPersistentNode(keyNodePath, null);
			LOGGER.info("Path {} is created",keyNodePath);
			Map<Object, Bucket<KeyValueFrequency>> valueBucketMap = keyToValueToBucketMap
					.get(keyName);
			for (Object valueKey : valueBucketMap.keySet()) {
				String buildValueBucketPath = keyNodePath;
				nodesManager.createPersistentNode(buildValueBucketPath, null);
				String buildValueKeyPath = buildValueBucketPath	+ PathUtil.FORWARD_SLASH + valueKey;
				nodesManager.createPersistentNode(buildValueKeyPath, null);
				String buildBucketPath = buildValueKeyPath + PathUtil.FORWARD_SLASH	+ valueBucketMap.get(valueKey).toString();
				nodesManager.createPersistentNode(buildBucketPath, null);
				//nodesManager.createPersistentNode(buildValueBucketPath, null);
				
				LOGGER.info("Path {} is created",buildValueBucketPath);
			}
		}
	}
	
	public static void createTrieBucketCombinationToNodeNumbersMap(
			Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap,
			NodesManager nodesManager) throws IOException {
		String buildPath = nodesManager
				.buildConfigPath("bucketCombinationToNodeNumbersMap");
		for (BucketCombination bucketCombinationKey : bucketCombinationToNodeNumbersMap
				.keySet()) {
			String keyNodePath = buildPath + PathUtil.FORWARD_SLASH
					+ bucketCombinationKey.toString();
			Set<Node> nodes = bucketCombinationToNodeNumbersMap
					.get(bucketCombinationKey);
			for (Node node : nodes) {
				String buildBucketCombinationNodeNumberPath = keyNodePath
						+ PathUtil.FORWARD_SLASH + node.toString();
				nodesManager.createPersistentNode(buildBucketCombinationNodeNumberPath, null);
				LOGGER.info("Path {} is created",buildBucketCombinationNodeNumberPath);
			}
		}
	}
}


