package com.talentica.hungryHippos.utility;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;

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

}
