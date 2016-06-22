/**
 * 
 */
package com.talentica.hungryHippos.node.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
/**
 * @author PooshanS
 *
 */
@Ignore
public class ConfigurationFileRetrieve {
	public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";
	public final static String bucketCombinationToNodeNumbersMapFile = "bucketCombinationToNodeNumbersMap";
	public final static String keyToValueToBucketMapFile = "keyToValueToBucketMap";
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationFileRetrieve.class.getName());
	
	private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap;
	private NodesManager nodesManager;
	
	@Before
	public void setUp() throws Exception{
		(nodesManager = NodesManagerContext.getNodesManagerInstance()).startup();
		keyToValueToBucketMap = new HashMap<String, Map<Object, Bucket<KeyValueFrequency>>>();
	}
	
	@Test
	public void getKeyToValueToBucketMapFile(){
		String buildPath = nodesManager.buildConfigPath(keyToValueToBucketMapFile);
		Set<LeafBean> leafs;
		try {
			leafs = ZKUtils.searchTree(buildPath, null, null);
			for(LeafBean leaf : leafs){
				LOGGER.info("Path is {} AND node name {}",leaf.getPath(),leaf.getName());
			}
		} catch (ClassNotFoundException | InterruptedException
				| KeeperException | IOException e) {
			LOGGER.info("Exception {}",e);
		}
		
	}

}
