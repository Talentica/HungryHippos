package com.talentica.hungryHippos.client.domain;

import org.junit.Assert;
import org.junit.Test;


public class ValueSetTest {

	@Test
	public void testToString() {
		ValueSet valueSet = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		String actual = valueSet.toString();
		Assert.assertEquals("ValueSet{0=India,1=Sony}", actual);
	}

	@Test
	public void testEquals() {
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		ValueSet valueSet2 = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		Assert.assertEquals(valueSet1, valueSet2);
	}

	@Test
	public void testEqualsForDifferentIndexes() {
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		ValueSet valueSet2 = new ValueSet(new int[] { 1, 0 }, new String[] { "India", "Sony" });
		Assert.assertNotEquals(valueSet1, valueSet2);
	}

	@Test
	public void testCompareToOfNotSameLengthValueSets() {
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		ValueSet valueSet2 = new ValueSet(new int[] { 1 }, new String[] { "India" });
		Assert.assertTrue(valueSet1.compareTo(valueSet2) > 0);
		Assert.assertTrue(valueSet2.compareTo(valueSet1) < 0);
	}

	@Test
	public void testCompareToOfSameValueSets() {
		MutableCharArrayString string1 = new MutableCharArrayString(2);
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new MutableCharArrayString[] { string1, string1 });
		ValueSet valueSet2 = new ValueSet(new int[] { 0, 1 }, new MutableCharArrayString[] { string1, string1 });
		Assert.assertTrue(valueSet1.compareTo(valueSet2) == 0);
		Assert.assertTrue(valueSet2.compareTo(valueSet1) == 0);
	}

	@Test
	public void testCompareToOfValueSetsOfSameSize() throws InvalidRowExeption {
		MutableCharArrayString country1 = new MutableCharArrayString(3);
		country1.addCharacter('I').addCharacter('n').addCharacter('d');
		MutableCharArrayString device1 = new MutableCharArrayString(3);
		device1.addCharacter('S').addCharacter('a').addCharacter('m');
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new MutableCharArrayString[] { country1, device1 });

		MutableCharArrayString country2 = new MutableCharArrayString(3);
		country2.addCharacter('I').addCharacter('n').addCharacter('d');
		MutableCharArrayString device2 = new MutableCharArrayString(3);
		device2.addCharacter('S').addCharacter('o').addCharacter('n');
		ValueSet valueSet2 = new ValueSet(new int[] { 0, 1 }, new MutableCharArrayString[] { country2, device2 });

		Assert.assertTrue(valueSet1.compareTo(valueSet2) < 0);
		Assert.assertTrue(valueSet2.compareTo(valueSet1) > 0);
	}

}
