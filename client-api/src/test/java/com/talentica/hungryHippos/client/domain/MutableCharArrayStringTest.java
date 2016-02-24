package com.talentica.hungryHippos.client.domain;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

public class MutableCharArrayStringTest {

	private MutableCharArrayString stringL1;

	private MutableCharArrayString stringL2;

	@Before
	public void setUp() {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(DataType.STRING, 1);
		stringL1 = new MutableCharArrayString(1);
		stringL1.addCharacter('l');
		stringL2 = new MutableCharArrayString(1);
		stringL2.addCharacter('l');
	}

	@Test
	public void testHashCode() {
		Assert.assertEquals(stringL1.hashCode(), stringL2.hashCode());
	}

	@Test
	public void testEquals() {
		Assert.assertTrue(stringL1.equals(stringL1));
		Assert.assertTrue(stringL2.equals(stringL2));
		Assert.assertTrue(stringL1.equals(stringL2));
		Assert.assertTrue(stringL2.equals(stringL1));
	}

	@Test
	public void testClone() {
		MutableCharArrayString testString = createTestString();
		MutableCharArrayString clonedString1 = testString.clone();
		Assert.assertNotNull(clonedString1);
		MutableCharArrayString expectedString1 = new MutableCharArrayString(2);
		expectedString1.addCharacter('l');
		expectedString1.addCharacter('c');
		Assert.assertEquals(expectedString1, clonedString1);
	}

	@Test
	public void testPutOfMutableCharArrayStringInMapAsKey() {
		MutableCharArrayString testString = createTestString();
		Map<MutableCharArrayString, Integer> mapOfKeyValueFrequencies = new HashMap<>();
		mapOfKeyValueFrequencies.put(testString, 0);
		Integer count = mapOfKeyValueFrequencies.get(testString);
		Assert.assertNotNull(count);
		Assert.assertEquals(0, count.intValue());
	}

	private MutableCharArrayString createTestString() {
		MutableCharArrayString testString = MutableCharArrayStringCache.getMutableStringFromCacheOfSize(2);
		testString.addCharacter('l');
		testString.addCharacter('c');
		return testString;
	}

	@Test
	public void testRetrievalOfMutableCharArrayStringKeyFromMap() {
		MutableCharArrayString testString = createTestString();
		Map<MutableCharArrayString, Integer> mapOfKeyValueFrequencies = new HashMap<>();
		mapOfKeyValueFrequencies.put(testString, 0);
		MutableCharArrayString testStringNew = createTestString();
		mapOfKeyValueFrequencies.put(testStringNew, 5);
		Integer count = mapOfKeyValueFrequencies.get(testString);
		Assert.assertNotNull(count);
		Assert.assertEquals(5, count.intValue());
	}

}