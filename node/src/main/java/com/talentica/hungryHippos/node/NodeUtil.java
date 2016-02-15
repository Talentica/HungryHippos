package com.talentica.hungryHippos.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.Bucket;
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

}
