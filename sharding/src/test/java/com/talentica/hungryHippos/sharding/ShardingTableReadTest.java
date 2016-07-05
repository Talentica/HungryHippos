package com.talentica.hungryHippos.sharding;

import org.junit.Test;

import com.talentica.hungryHippos.sharding.utils.ShardingTableReadService;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingTableReadTest {

	@Test
	public void testBucketCombinationToNode(){
		ShardingTableReadService reader = new ShardingTableReadService();
		reader.readBucketCombinationToNodeNumbersMap();
	}
	
}
