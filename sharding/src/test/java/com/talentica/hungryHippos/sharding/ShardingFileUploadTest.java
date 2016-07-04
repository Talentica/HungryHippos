/**
 * 
 */
package com.talentica.hungryHippos.sharding;

import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.sharding.utils.ShardingTableUploadService;
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;

/**
 * @author pooshans
 *
 */
public class ShardingFileUploadTest {

	private ShardingTableUploadService service;

	@Before
	public void setUp() throws Exception {
		String flag = CoordinationApplicationContext.getZkProperty().getValueByKey("cleanup.zookeeper.nodes");
		if (flag.equals("Y")) {
			ObjectFactory factory = new ObjectFactory();
			CoordinationServers coordinationServers = factory.createCoordinationServers();
			service = new ShardingTableUploadService(coordinationServers);
			NodesManagerContext.getNodesManagerInstance(coordinationServers).startup();
		}
	}

	@Test
	@Ignore
	public void testBucketCombinationToNode()
			throws IOException, InterruptedException, IllegalArgumentException, IllegalAccessException {
		service.zkUploadBucketCombinationToNodeNumbersMap();
	}

	@Test
	public void testBucketToNodeNumber()
			throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
		service.zkUploadBucketToNodeNumberMap();
	}
}
