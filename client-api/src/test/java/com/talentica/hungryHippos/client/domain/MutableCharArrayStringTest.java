package com.talentica.hungryHippos.client.domain;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

public class MutableCharArrayStringTest {

	private MutableCharArrayString stringL1;

	private MutableCharArrayString stringL2;

	@Before
	public void setUp() {
		FieldTypeArrayDataDescription dataDescription= new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(DataType.STRING, 1);
		stringL1 = new MutableCharArrayString(new ByteBuffer(dataDescription));
		stringL1.addCharacter('l');
		stringL2 = new MutableCharArrayString(new ByteBuffer(dataDescription));
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
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(DataType.STRING, 2);
		MutableCharArrayString testString = new MutableCharArrayString(new ByteBuffer(dataDescription));
		testString.addCharacter('l');
		testString.addCharacter('c');
		MutableCharArrayString clonedString1 = testString.clone();
		Assert.assertNotNull(clonedString1);
		MutableCharArrayString expectedString1 = new MutableCharArrayString(new ByteBuffer(dataDescription));
		expectedString1.addCharacter('l');
		expectedString1.addCharacter('c');
		Assert.assertEquals(expectedString1, clonedString1);
	}

}
