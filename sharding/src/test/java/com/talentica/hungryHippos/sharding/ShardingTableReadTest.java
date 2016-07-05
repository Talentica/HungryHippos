package com.talentica.hungryHippos.sharding;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.junit.Test;

import com.talentica.hungryHippos.sharding.utils.ShardingTableReadService;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingTableReadTest {

	@Test
	public void testBucketCombinationToNode() throws FileNotFoundException, JAXBException {
		ShardingTableReadService reader = new ShardingTableReadService();
		reader.readBucketCombinationToNodeNumbersMap();
	}
	
}
