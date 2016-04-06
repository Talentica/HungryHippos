/**
 * 
 */
package com.talentica.hungryHippos.node.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.node.NodeUtil;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class ConfigurationFileUploadTest {

	private NodesManager nodesManager;
	private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap;
	private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;
	private Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationFileUploadTest.class.getName());
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception{
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		//(nodesManager = ServerHeartBeat.init()).startup();
		nodesManager = CommonUtil.connectZK();
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
		
		try (ObjectInputStream bucketCombinationToNodeNumbersMapInputStream = new ObjectInputStream(
				new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH
						+ Sharding.bucketCombinationToNodeNumbersMapFile))) {
			bucketCombinationToNodeNumbersMap = (Map<BucketCombination, Set<Node>>) bucketCombinationToNodeNumbersMapInputStream
					.readObject();
		} catch (IOException | ClassNotFoundException e) {
			LOGGER.info("Unable to read bucketCombinationToNodeNumbersMap. Please put the file in current directory");
		}
	
	}
	
	@Test
	public void createUploadConfigFile() throws IOException, InterruptedException{
		NodeUtil.createTrieBucketToNodeNumberMap(bucketToNodeNumberMap, nodesManager);
		NodeUtil.createTrieKeyToValueToBucketMap(keyToValueToBucketMap, nodesManager);
		NodeUtil.createTrieBucketCombinationToNodeNumbersMap(bucketCombinationToNodeNumbersMap, nodesManager);
		nodesManager.createPersistentNode("/rootnode/hostsnode/test/child",null);
		Thread.sleep(3000);
		
		//String buildPath = nodesManager.buildConfigPath("/rootnode/hostsnode/test/child");
		Set<LeafBean> leafs;
		try {
			leafs = ZKUtils.searchTree("/rootnode/hostsnode/test", null, null);
			for(LeafBean leaf : leafs){
				LOGGER.info("Path is {} AND node name {}",leaf.getPath(),leaf.getName());
			}
		} catch (ClassNotFoundException | InterruptedException
				| KeeperException | IOException e) {
			LOGGER.info("Exception {}",e);
		}
	}
	
}
