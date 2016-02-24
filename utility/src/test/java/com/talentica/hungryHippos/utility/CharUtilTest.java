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
	
	private static String expected1 = "ab";
	private static String expected2 = "zxrf";
	char[] chrs1;
	char[] chrs2;
	int bufferSize;
	
	
	@Before
	public void setUp(){
		chrs1 = "ab".toCharArray();
		chrs2 = "zxrf".toCharArray();
	}
	
	@Test
	public void testEqualChars(){
		int value1 = CharUtil.getIntValue(chrs1);
		Assert.assertEquals(expected1, new String(CharUtil.getCharArrays(value1)).trim());
		int value2 = CharUtil.getIntValue(chrs2);
		Assert.assertEquals(expected2, new String(CharUtil.getCharArrays(value2)));
	}

}
