package com.talentica.hungryHippos.utility;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

public class MutableCharArrayStringTest {

	private MutableCharArrayString stringL1;

	private MutableCharArrayString stringL2;

	@Before
	public void setUp() {
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
		MutableCharArrayString testString = new MutableCharArrayString(2);
		testString.addCharacter('l');
		testString.addCharacter('c');
		MutableCharArrayString clonedString1 = testString.clone();
		Assert.assertNotNull(clonedString1);
		MutableCharArrayString expectedString1 = new MutableCharArrayString(2);
		expectedString1.addCharacter('l');
		expectedString1.addCharacter('c');
		Assert.assertEquals(expectedString1, clonedString1);
		testString.reset();
		testString.addCharacter('m');

		MutableCharArrayString clonedString2 = testString.clone();
		Assert.assertNotNull(clonedString2);
		MutableCharArrayString expectedString2 = new MutableCharArrayString(2);
		expectedString2.addCharacter('m');
		Assert.assertEquals(expectedString2, clonedString2);
	}

	@Test
	public void testReset() {

	}

}
