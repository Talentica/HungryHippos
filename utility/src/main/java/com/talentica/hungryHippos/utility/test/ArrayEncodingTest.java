/**
 * 
 */
package com.talentica.hungryHippos.utility.test;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.ArrayEncoder;

/**
 * @author PooshanS
 *
 */
public class ArrayEncodingTest {
	
	int result ;
	int[] keysIndex1;
	
	@Before
	public void setUp(){
		result = 0;
		keysIndex1 = new int[]{0,1,3};
	}
	
	@Test
	public void testEncoding(){
		int encodeValue = ArrayEncoder.encode(keysIndex1);
		Assert.assertTrue(11 == encodeValue);
		int[] decodedValue = ArrayEncoder.decode(encodeValue);
		Assert.assertTrue(Arrays.equals(keysIndex1, decodedValue));
	}
	
}
