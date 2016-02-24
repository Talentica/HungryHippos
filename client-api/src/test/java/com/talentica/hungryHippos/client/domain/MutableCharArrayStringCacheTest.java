package com.talentica.hungryHippos.client.domain;

import org.junit.Assert;
import org.junit.Test;

public class MutableCharArrayStringCacheTest {

	@Test
	public void testGetMutableStringFromCacheOfSameSize() {
		MutableCharArrayString arrayString1 = MutableCharArrayStringCache.getMutableStringFromCacheOfSize(1);
		Assert.assertNotNull(arrayString1);
		MutableCharArrayString arrayString2 = MutableCharArrayStringCache.getMutableStringFromCacheOfSize(1);
		Assert.assertNotNull(arrayString2);
		Assert.assertTrue(arrayString1 == arrayString2);
	}

	@Test
	public void testGetMutableStringFromCacheOfDifferentSize() {
		MutableCharArrayString arrayString1 = MutableCharArrayStringCache.getMutableStringFromCacheOfSize(1);
		Assert.assertNotNull(arrayString1);
		MutableCharArrayString arrayString2 = MutableCharArrayStringCache.getMutableStringFromCacheOfSize(2);
		Assert.assertNotNull(arrayString2);
		Assert.assertFalse(arrayString1 == arrayString2);
	}

	@Test
	public void testGetMutableStringFromCacheAndChangeCharactersInIt() {
		MutableCharArrayString arrayString1 = MutableCharArrayStringCache.getMutableStringFromCacheOfSize(4);
		Assert.assertNotNull(arrayString1);
		arrayString1.addCharacter('a');
		arrayString1.addCharacter('b');
		arrayString1.addCharacter('c');
		arrayString1.addCharacter('d');
		Assert.assertEquals(4, arrayString1.length());
		MutableCharArrayString arrayString2 = MutableCharArrayStringCache.getMutableStringFromCacheOfSize(4);
		arrayString2.addCharacter('e');
		Assert.assertEquals(1, arrayString2.length());
	}

}
