package com.talentica.hungryHippos.client.domain;

import org.junit.Assert;
import org.junit.Test;


public class ValueSetTest {

	@Test
	public void testToString() {
		ValueSet valueSet = new ValueSet(new int[] { 0, 1 }, new Object[] { "India", "Sony" });
		String actual = valueSet.toString();
		Assert.assertEquals("ValueSet{Country=India,Device=Sony}", actual);
	}

}
