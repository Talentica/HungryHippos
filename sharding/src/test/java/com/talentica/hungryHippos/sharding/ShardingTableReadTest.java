package com.talentica.hungryHippos.sharding;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.sharding.utils.ShardingTableReadService;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingTableReadTest {

	private ShardingTableReadService reader;
	
	@Before
	  public void setUp() throws Exception {
	    reader = new ShardingTableReadService();
	  }
	
	@Test
	public void testBucketCombinationToNode(){
		try{
			reader.readBucketCombinationToNodeNumbersMap();
			Assert.assertTrue(true);
		}catch(Exception e){
			Assert.assertFalse(false);
		}
	}
	
	@Test
	public void testBucketToNodeNumber(){
		try{
			reader.readBucketToNodeNumberMap();
			Assert.assertTrue(true);
		}catch(Exception e){
			Assert.assertFalse(false);
		}
	}
	
	@Test
	public void testKeyToValueToBucket(){
		try{
			reader.readKeyToValueToBucketMap();
			Assert.assertTrue(true);
		}catch(Exception e){
			Assert.assertFalse(false);
		}
	}
	
}
