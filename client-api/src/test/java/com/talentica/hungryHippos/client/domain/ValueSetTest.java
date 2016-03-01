package com.talentica.hungryHippos.client.domain;

import org.junit.Assert;
import org.junit.Test;


public class ValueSetTest {

	@Test
	public void testToString() {
		ValueSet valueSet = new ValueSet(new int[] { 0, 1 }, new Object[] { "India", "Sony" });
		String actual = valueSet.toString();
		Assert.assertEquals("ValueSet{0=India,1=Sony}", actual);
	}

	@Test
	public void testEquals() {
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new Object[] { "India", "Sony" });
		ValueSet valueSet2 = new ValueSet(new int[] { 0, 1 }, new Object[] { "India", "Sony" });
		Assert.assertEquals(valueSet1, valueSet2);
	}

	@Test
	public void testEqualsForDifferentIndexes() {
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new Object[] { "India", "Sony" });
		ValueSet valueSet2 = new ValueSet(new int[] { 1, 0 }, new Object[] { "India", "Sony" });
		Assert.assertNotEquals(valueSet1, valueSet2);
	}

}
