/**
 * 
 */
package com.talentica.hungryHippos.utility;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author PooshanS
 *
 */
public class CharUtilTest {
	
	private static String expected = "abcd";
	char[] chrs;
	int bufferSize;
	
	
	@Before
	public void setUp(){
		chrs = "abcd".toCharArray();
	}
	
	@Test
	public void testEqualChars(){
		int value = CharUtil.getIntValue(chrs);
		Assert.assertEquals(expected, new String(CharUtil.getCharArrays(value)));
	}

}
